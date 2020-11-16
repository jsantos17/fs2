package fs2
package interop
package reactivestreams

import cats._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import org.reactivestreams._

import fs2.concurrent.{Queue, NoneTerminatedQueue}

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
    Queue.boundedNoneTerminated[F, Either[Throwable, A]](1).flatMap(
      fsm[F, A](_, 1).map(new StreamSubscriber(_)))

  def apply[F[_]: ConcurrentEffect, A](batchSize: Int): F[StreamSubscriber[F, A]] =
    Queue.boundedNoneTerminated[F, Either[Throwable, A]](batchSize).flatMap(
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
    def dequeue1: F[Option[Chunk[Either[Throwable, A]]]]

    /** downstream stream */
    def stream(subscribe: F[Unit])(implicit ev: ApplicativeError[F, Throwable]): Stream[F, A] =
      Stream.bracket(subscribe)(_ => onFinalize) >> Stream
        .eval(dequeue1)
        .repeat
        .unNone
        .flatMap(Stream.chunk)
        .rethrow
  }

  private[reactivestreams] def fsm[F[_], A](q: NoneTerminatedQueue[F, Either[Throwable, A]], batchSize: Int)(
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
    case object Receiving extends State
    case object RequestBeforeSubscription extends State
    case class WaitingOnUpstream(sub: Subscription) extends State
    case object UpstreamCompletion extends State
    case object DownstreamCancellation extends State
    case class UpstreamError(err: Throwable) extends State

    def step(in: Input): State => (State, F[Unit]) =
      in match {
        case OnSubscribe(s) => {
          case RequestBeforeSubscription => WaitingOnUpstream(s) -> F.delay(s.request(batchSize))
          case Uninitialized => Idle(s) -> F.unit
          case o =>
            val err = new Error(s"received subscription in invalid state [$o]")
            o -> (F.delay(s.cancel) >> F.raiseError(err))
        }
        case OnNext(a) => {
          case WaitingOnUpstream(_) if batchSize > 1 => Receiving -> q.enqueue1(a.asRight.some)
          case WaitingOnUpstream(s) if batchSize === 1 => Idle(s) -> (q.enqueue1(a.asRight.some) >> q.enqueue1(None))
          case Receiving => Receiving -> q.enqueue1(a.asRight.some)
          case DownstreamCancellation => DownstreamCancellation -> F.unit
          case o => o -> F.raiseError(new Error(s"received record [$a] in invalid state [$o]"))
        }
        case OnComplete => {
          case WaitingOnUpstream(_) => UpstreamCompletion -> q.enqueue1(None)
          case _ => UpstreamCompletion -> F.unit
        }
        case OnError(e) => {
          case WaitingOnUpstream(_) => UpstreamError(e) -> q.enqueue1(e.asLeft.some)
          case _ => UpstreamError(e) -> F.unit
        }
        case OnFinalize => {
          case WaitingOnUpstream(sub) => DownstreamCancellation -> (F.delay(sub.cancel) >> q.enqueue1(None))
          case Idle(sub) => DownstreamCancellation -> F.delay(sub.cancel)
          case o => o -> F.unit
        }
        case OnDequeue => {
          case Uninitialized           => RequestBeforeSubscription -> F.unit
          case Idle(sub)               => WaitingOnUpstream(sub) -> F.delay(sub.request(batchSize))
          case Receiving               => Receiving -> F.unit
          case err @ UpstreamError(e)  => err -> q.enqueue1(e.asLeft.some)
          case UpstreamCompletion      => UpstreamCompletion -> q.enqueue1(None)
          case o                       => o -> q.enqueue1((new Error(s"received request in invalid state [$o]")).asLeft.some)
        }
      }

    Ref.of[F, State](Uninitialized) map { ref =>
      new FSM[F, A] {
        def nextState(in: Input): F[Unit] = ref.modify(step(in)).flatten
        def onSubscribe(s: Subscription): F[Unit] = nextState(OnSubscribe(s))
        def onNext(a: A): F[Unit] = nextState(OnNext(a))
        def onError(t: Throwable): F[Unit] = nextState(OnError(t))
        def onComplete: F[Unit] = nextState(OnComplete)
        def onFinalize: F[Unit] = nextState(OnFinalize)
        def dequeue1: F[Option[Chunk[Either[Throwable, A]]]] =
          ref.modify(step(OnDequeue)).flatten >> q.dequeueChunk1(batchSize)
      }
    }
  }
}
