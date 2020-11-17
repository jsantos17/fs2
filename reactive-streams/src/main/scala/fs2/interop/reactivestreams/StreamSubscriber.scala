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
    Queue.bounded[F, Either[Throwable, Option[A]]](1).flatMap(
      fsm[F, A](_, 1).map(new StreamSubscriber(_)))

  def apply[F[_]: ConcurrentEffect, A](batchSize: Int): F[StreamSubscriber[F, A]] =
    Queue.bounded[F, Either[Throwable, Option[A]]](batchSize).flatMap(
      fsm[F, A](_, batchSize).map(new StreamSubscriber(_)))

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
    def dequeue1: F[Chunk[Either[Throwable, Option[A]]]]

    /** downstream stream */
    def stream(subscribe: F[Unit])(implicit ev: ApplicativeError[F, Throwable]): Stream[F, A] =
      Stream.bracket(subscribe)(_ => onFinalize) >> Stream
        .evalUnChunk(dequeue1)
        .repeat
        .rethrow
        .unNoneTerminate
  }

  private[reactivestreams] def fsm[F[_], A](q: Queue[F, Either[Throwable, Option[A]]], batchSize: Int)(
    implicit F: Concurrent[F])
      : F[FSM[F, A]] = {

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
    case class NextBatch(sub: Subscription) extends State
    case class Receiving(sub: Subscription) extends State
    case object RequestBeforeSubscription extends State
    case class WaitingOnUpstream(sub: Subscription) extends State
    case object UpstreamCompletion extends State
    case object DownstreamCancellation extends State
    case class UpstreamError(err: Throwable) extends State

    def step(in: Input, counter: Int): State => (State, F[Unit]) =
      in match {
        case OnSubscribe(s) => {
          case RequestBeforeSubscription => WaitingOnUpstream(s) -> F.delay(s.request(batchSize))
          case Uninitialized => Idle(s) -> F.unit
          case o =>
            val err = new Error(s"received subscription in invalid state [$o]")
            o -> (F.delay(s.cancel) >> F.raiseError(err))
        }
        case OnNext(a) => {
          case WaitingOnUpstream(s) if batchSize > 1 => Receiving(s) -> q.enqueue1(a.some.asRight)
          case WaitingOnUpstream(s) if batchSize === 1 => Idle(s) -> q.enqueue1(a.some.asRight)
          case Receiving(s) if counter % batchSize === 0 => NextBatch(s) -> q.enqueue1(a.some.asRight)
          case Receiving(s) => Receiving(s) -> q.enqueue1(a.some.asRight)
          case DownstreamCancellation => DownstreamCancellation -> F.unit
          case o => o -> F.raiseError(new Error(s"received record [$a] in invalid state [$o]"))
        }
        case OnComplete => {
          case WaitingOnUpstream(_) => UpstreamCompletion -> q.enqueue1(None.asRight)
          case Receiving(_) => UpstreamCompletion -> q.enqueue1(None.asRight)
          case _ => UpstreamCompletion -> F.unit
        }
        case OnError(e) => {
          case WaitingOnUpstream(_) => UpstreamError(e) -> (q.enqueue1(e.asLeft) >> q.enqueue1(None.asRight))
          case Receiving(_) => UpstreamCompletion -> q.enqueue1(None.asRight)
          case _ => UpstreamError(e) -> F.unit
        }
        case OnFinalize => {
          case WaitingOnUpstream(sub) =>
            DownstreamCancellation -> (F.delay(sub.cancel) >> q.enqueue1(None.asRight))
          case Idle(sub) => DownstreamCancellation -> F.delay(sub.cancel)
          case o => o -> F.unit
        }
        case OnDequeue => {
          case Uninitialized =>
            RequestBeforeSubscription -> F.unit
          case Idle(sub) =>
            WaitingOnUpstream(sub) -> F.delay(sub.request(batchSize))
          case NextBatch(sub) =>
            Receiving(sub) -> F.delay(sub.request(batchSize))
          case Receiving(sub) =>
            Receiving(sub) -> F.unit
          case err @ UpstreamError(e) =>
            err -> (q.enqueue1(e.asLeft) >> q.enqueue1(None.asRight))
          case UpstreamCompletion =>
            UpstreamCompletion -> q.enqueue1(None.asRight)
          case o =>
            o -> (q.enqueue1((new Error(s"received request in invalid state [$o]")).asLeft) >> q.enqueue1(None.asRight))
        }
      }

    (Ref.of[F, State](Uninitialized), Ref.of[F, Int](0)) mapN {
      case (ref, counter) =>
        new FSM[F, A] {
          def nextState(in: Input): F[Unit] = counter.get.flatMap(c => ref.modify(step(in, c)).flatten)
          def onSubscribe(s: Subscription): F[Unit] =
            Concurrent[F].delay(println("Calling onSubscribe")) >> nextState(OnSubscribe(s))
          def onNext(a: A): F[Unit] =
            Concurrent[F].delay(println("Calling onNext")) >> counter.update(_ + 1) >> nextState(OnNext(a))
          def onError(t: Throwable): F[Unit] =
            Concurrent[F].delay(println("Calling onError")) >> nextState(OnError(t))
          def onComplete: F[Unit] =
            Concurrent[F].delay(println("Calling onComplete")) >> nextState(OnComplete)
          def onFinalize: F[Unit] =
            Concurrent[F].delay(println("Calling onFinalize")) >> nextState(OnFinalize)
          def dequeue1: F[Chunk[Either[Throwable, Option[A]]]] =
            Concurrent[F].delay(println("Calling dequeue1")) >>
              counter.get.flatMap(c => ref.modify(step(OnDequeue, c)).flatten) >> q.dequeueChunk1(batchSize)
        }
    }
  }
}
