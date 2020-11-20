package fs2
package interop
package reactivestreams

import cats._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import org.reactivestreams._

import fs2.concurrent.Queue

/**
  * Implementation of a `org.reactivestreams.Subscriber`.
  *
  * This is used to obtain a `fs2.Stream` from an upstream reactivestreams system.
  *
  * @see [[https://github.com/reactive-streams/reactive-streams-jvm#2-subscriber-code]]
  */
final class StreamSubscriber[F[_]: ConcurrentEffect, A](val sub: StreamSubscriber.FSM[F, A])
    extends Subscriber[A] {

  /** Called by an upstream reactivestreams system */
  def onSubscribe(s: Subscription): Unit = {
    nonNull(s)
    sub.onSubscribe(s).unsafeRunAsync()
  }

  /** Called by an upstream reactivestreams system */
  def onNext(a: A): Unit = {
    nonNull(a)
    sub.onNext(a).unsafeRunAsync()
  }

  /** Called by an upstream reactivestreams system */
  def onComplete(): Unit = sub.onComplete.unsafeRunAsync()

  /** Called by an upstream reactivestreams system */
  def onError(t: Throwable): Unit = {
    nonNull(t)
    sub.onError(t).unsafeRunAsync()
  }

  /** Obtain a fs2.Stream */
  @deprecated(
    "subscribing to a publisher prior to pulling the stream is unsafe if interrupted",
    "2.2.3"
  )
  def stream: Stream[F, A] = stream(().pure[F])

  def stream(subscribe: F[Unit]): Stream[F, A] = sub.stream(subscribe)

  private def nonNull[B](b: B): Unit = if (b == null) throw new NullPointerException()
}

object StreamSubscriber {
  def apply[F[_]: ConcurrentEffect, A]: F[StreamSubscriber[F, A]] =
    Queue
      .bounded[F, Either[Throwable, Option[A]]](2)
      .flatMap(fsm[F, A](_, 1).map(new StreamSubscriber(_)))

  def apply[F[_]: ConcurrentEffect, A](batchSize: Int): F[StreamSubscriber[F, A]] =
    Queue
      .bounded[F, Either[Throwable, Option[A]]](batchSize * 2)
      .flatMap(fsm[F, A](_, batchSize).map(new StreamSubscriber(_)))

  /** A finite state machine describing the subscriber */
  private[reactivestreams] trait FSM[F[_], A] {

    /** receives a subscription from upstream */
    def onSubscribe(s: Subscription): F[Unit]

    /** receives next record from upstream */
    def onNext(a: A): F[Unit]

    /** receives error from upstream */
    def onError(t: Throwable): F[Unit]

    /** called when upstream has finished sending records */
    def onComplete: F[Unit]

    /** called when downstream has finished consuming records */
    def onFinalize: F[Unit]

    /** producer for downstream */
    def dequeueChunk1: F[Chunk[Either[Throwable, Option[A]]]]

    /** downstream stream */
    def stream(subscribe: F[Unit])(implicit ev: ApplicativeError[F, Throwable]): Stream[F, A] =
      Stream.bracket(subscribe)(_ => onFinalize) >> Stream
        .evalUnChunk(dequeueChunk1)
        .repeat
        .rethrow
        .unNoneTerminate
  }

  private[reactivestreams] def fsm[F[_], A](
      q: Queue[F, Either[Throwable, Option[A]]],
      batchSize: Int
  )(implicit F: Concurrent[F]): F[FSM[F, A]] = {

    sealed trait Input
    case class OnSubscribe(s: Subscription) extends Input
    case class OnNext(a: A) extends Input
    case class OnError(e: Throwable) extends Input
    case object OnComplete extends Input
    case object OnFinalize extends Input
    case object OnDequeue extends Input

    sealed trait State
    case object Uninitialized extends State
    case class Idle(sub: Subscription) extends State
    case class Receiving(sub: Subscription) extends State
    case object RequestBeforeSubscription extends State
    case class WaitingOnUpstream(sub: Subscription) extends State
    case object UpstreamCompletion extends State
    case object DownstreamCancellation extends State
    case class UpstreamError(err: Throwable) extends State

    def step(in: Input): State => F[State] =
      in match {
        case OnSubscribe(s) => {
          case RequestBeforeSubscription =>
            F.delay(s.request(batchSize)).as(WaitingOnUpstream(s))
          case Uninitialized =>
            (Idle(s): State).pure[F]
          case Idle(_) =>
            F.delay(s.cancel).as(Idle(s))
          case o =>
            val err = new Error(s"received subscription in invalid state [$o]")
            (F.delay(s.cancel) >> F.raiseError(err)).as(o)
        }
        case OnNext(a) => {
          case WaitingOnUpstream(s) =>
            q.enqueue1(a.some.asRight).as(Receiving(s))
          case Receiving(s) =>
            q.enqueue1(a.some.asRight).as(Receiving(s))
          case Idle(s) =>
            q.enqueue1(a.some.asRight).as(Receiving(s))
          case DownstreamCancellation =>
            (DownstreamCancellation: State).pure[F]
          case o =>
            F.raiseError(new Error(s"received record [$a] in invalid state [$o]")).as(o)
        }
        case OnComplete => {
          case WaitingOnUpstream(_) =>
            q.enqueue1(None.asRight).as(UpstreamCompletion)
          case Receiving(_) =>
            q.enqueue1(None.asRight).as(UpstreamCompletion)
          case Idle(_) =>
            q.enqueue1(None.asRight).as(UpstreamCompletion)
          case _ =>
            (UpstreamCompletion: State).pure[F]
        }
        case OnError(e) => {
          case WaitingOnUpstream(_) =>
            (q.enqueue1(e.asLeft) >> q.enqueue1(None.asRight)).as(UpstreamError(e))
          case Receiving(_) =>
            q.enqueue1(None.asRight).as(UpstreamError(e))
          case _ => (UpstreamError(e): State).pure[F]
        }
        case OnFinalize => {
          case WaitingOnUpstream(sub) =>
            (F.delay(sub.cancel) >> q.enqueue1(None.asRight)).as(DownstreamCancellation)
          case Idle(sub) =>
            F.delay(sub.cancel).as(DownstreamCancellation)
          case Receiving(sub) =>
            F.delay(sub.cancel).as(DownstreamCancellation)
          case o =>
            o.pure[F]
        }
        case OnDequeue => {
          case Uninitialized =>
            (RequestBeforeSubscription: State).pure[F]
          case Receiving(sub) =>
            (Receiving(sub): State).pure[F]
          case err @ UpstreamError(e) =>
            (q.enqueue1(e.asLeft) >> q.enqueue1(None.asRight)).as(err)
          case Idle(sub) =>
            F.delay(sub.request(batchSize)).as(WaitingOnUpstream(sub)) // request on first dequeue
          case UpstreamCompletion =>
            q.enqueue1(None.asRight).as(UpstreamCompletion)
          case st => st.pure[F]
        }
      }

    Ref.of[F, State](Uninitialized) map { ref =>
      new FSM[F, A] {
        def nextState(in: Input): F[Unit] =
          ref.get
            .flatMap(step(in))
            .flatMap(ref.set)
        def onSubscribe(s: Subscription): F[Unit] =
          nextState(OnSubscribe(s))
        def onNext(a: A): F[Unit] =
          nextState(OnNext(a))
        def onError(t: Throwable): F[Unit] =
          nextState(OnError(t))
        def onComplete: F[Unit] =
          nextState(OnComplete)
        def onFinalize: F[Unit] =
          nextState(OnFinalize)
        def dequeueChunk1: F[Chunk[Either[Throwable, Option[A]]]] =
          for {
            st <- ref.get
            updated <- step(OnDequeue)(st)
            chunk <- q.dequeueChunk1(batchSize)
            _ <- ref.set(updated)
            _ <- st match {
              case WaitingOnUpstream(s) => F.delay(s.request(batchSize))
              case Receiving(s) => F.delay(s.request(batchSize))
              case Idle(s) => F.delay(s.request(batchSize))
              case _ => q.enqueue1(None.asRight)
            }
          } yield chunk
      }
    }
  }
}
