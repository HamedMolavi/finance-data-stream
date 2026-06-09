package pipeline

import (
	"reflect"
	"runtime"
	"sync"

	"github.com/HamedMolavi/finance-data-stream/utils"
)

// MapStageFactory applies fn to each value received from inbound and emits the results
// on a returned outbound channel.
//
// It creates exactly 1 goroutine, which sequentially:
//   - reads values from inbound,
//   - applies fn to each value, and
//   - sends the result to outbound.
//
// The outbound channel is closed when the inbound channel is closed and all
// values from it have been processed. Specifically, once the range loop over
// inbound completes, the goroutine exits and closes outbound via defer.
//
// MapStageFactory does not introduce parallelism; processing is performed serially in a
// single goroutine.
func MapStageFactory[I, O any](fn func(item I) O) ProcessStage[I, O] {
	nilable := utils.IsNilable(*new(O))
	return func(inbound Stream[I], opts ...StageOption) Stream[O] {
		cfg := defaultConfig()
		for _, opt := range opts {
			opt(cfg)
		}
		outbound := make(chan O, cap(inbound))
		go func() {
			defer close(outbound)
			for d := range inbound {
				if o := fn(d); !cfg.DropNil || (nilable && utils.IsNil(o)) {
					outbound <- o
				}
			}
		}()
		return outbound
	}
}

func TryMapStageFactory[I, O any](fn func(item I) (O, error)) TryProcessStage[I, O] {
	nilable := utils.IsNilable(*new(O))
	return func(eFunc ErrorRegistererFunc, inbound Stream[I], opts ...StageOption) Stream[O] {
		cfg := CreateConfig(opts)
		outbound := make(chan O, cap(inbound))
		errCh := make(chan *Error, cap(inbound))
		eFunc(errCh)
		go func() {
			defer close(outbound)
			defer close(errCh)
			for d := range inbound {
				o, err := fn(d)
				if err != nil {
					errCh <- &Error{
						Error:       err,
						StageConfig: cfg,
						Input:       d,
						Output:      o,
					}
				} else if !cfg.DropNil || (nilable && utils.IsNil(o)) {
					outbound <- o
				}
			}
		}()
		return outbound
	}
}

// MapParallel applies fn to each value received from inbound and emits the
// results on a returned outbound channel, using goroutines for concurrency.
//
// Goroutines:
//   - One coordinator goroutine is always created.
//   - If degree <= 0: one additional goroutine is spawned per inbound item
//     (unbounded concurrency).
//   - If degree > 0: one goroutine is spawned per inbound item, but at most
//     `degree` goroutines execute fn concurrently (bounded by a semaphore).
//
// Ordering:
//   - Output order is not guaranteed to match input order.
//
// Channel closure:
//   - The outbound channel is closed after:
//     1. The inbound channel has been closed and fully drained, and
//     2. All spawned worker goroutines have completed and sent their results.
//   - No sends occur after outbound is closed.
//
// Notes:
//   - Backpressure may occur if outbound is not being consumed.
//   - Spawning a goroutine per item may lead to high memory usage if inbound
//     is large or unbounded.
func MapParallelStageFactory[I, O any](fn func(item I) O, degree int) ProcessStage[I, O] {
	nilable := utils.IsNilable(*new(O))
	cfg := defaultConfig()
	return func(inbound Stream[I], opts ...StageOption) Stream[O] {
		for _, opt := range opts {
			opt(cfg)
		}
		outbound := make(chan O, cap(inbound))
		if degree <= 0 {
			go func() {
				wg := sync.WaitGroup{}
				defer close(outbound)
				defer wg.Wait()
				for d := range inbound {
					wg.Add(1)
					go func(d I) {
						defer wg.Done()
						if o := fn(d); !cfg.DropNil || (nilable && utils.IsNil(o)) {
							outbound <- o
						}
					}(d)
				}
			}()
		} else {
			go func() {
				wg := sync.WaitGroup{}
				wg2 := make(chan struct{}, degree)
				defer close(outbound)
				defer wg.Wait()
				for d := range inbound {
					wg.Add(1)
					wg2 <- struct{}{}
					go func(d I) {
						defer wg.Done()
						defer func() { <-wg2 }()
						if o := fn(d); !cfg.DropNil || (nilable && utils.IsNil(o)) {
							outbound <- o
						}
					}(d)
				}
			}()
		}
		return outbound
	}
}

func MapParallel2StageFactory[I, O any](fn func(item I) O, degree int) ProcessStage[I, O] {
	cfg := defaultConfig()
	nilable := utils.IsNilable(*new(O))
	return func(inbound Stream[I], opts ...StageOption) Stream[O] {
		for _, opt := range opts {
			opt(cfg)
		}
		outbound := make(chan O, cap(inbound))
		if degree <= 0 {
			degree = runtime.NumCPU()
		}
		wg := &sync.WaitGroup{}
		go func() {
			defer close(outbound)
			defer wg.Wait()

			for range degree {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for d := range inbound {
						if o := fn(d); !cfg.DropNil || (nilable && utils.IsNil(o)) {
							outbound <- o
						}
					}
				}()
			}

		}()

		return outbound
	}
}

// MapParallelOrdered applies fn to each value received from inbound and emits
// results on a returned outbound channel while preserving input order.
//
// Concurrency model:
//   - Two long-lived coordinator goroutines are created:
//     1. A worker coordinator that reads inbound and spawns workers.
//     2. An ordering coordinator that reorders results from interCh and
//     forwards them to outbound.
//   - Additionally, one worker goroutine is spawned per inbound item.
//
// Concurrency limits:
//   - If degree <= 0: all worker goroutines may execute concurrently
//     (unbounded concurrency).
//   - If degree > 0: at most `degree` worker goroutines execute fn concurrently,
//     enforced via a semaphore channel.
//
// Ordering:
//   - Each input item is assigned a monotonically increasing index.
//   - Results may complete out of order, but are buffered and emitted strictly
//     in input order by the ordering coordinator.
//
// Channel closure:
//   - interCh is closed after:
//     1. inbound is closed and fully drained, and
//     2. all worker goroutines have completed.
//   - outbound is closed after:
//     1. interCh is closed, and
//     2. all buffered results have been emitted in order.
//
// Guarantees:
//   - No values are sent to outbound after it is closed.
//   - Output order matches input order exactly.
//
// Notes:
//   - Memory usage may grow with the number of out-of-order completed results
//     waiting in the reorder buffer.
//   - Spawning one goroutine per item may be expensive for very large or
//     unbounded streams.
func MapParallelOrderedStageFactory[I, O any](fn func(item I) O, degree int) ProcessStage[I, O] {
	type wrap struct {
		i uint64
		d O
	}
	cfg := defaultConfig()
	nilable := utils.IsNilable(*new(O))
	return func(inbound Stream[I], opts ...StageOption) Stream[O] {
		for _, opt := range opts {
			opt(cfg)
		}
		interCh := make(chan *wrap, cap(inbound)*2)
		outbound := make(chan O, cap(inbound))
		go func() {
			defer close(outbound)
			supposedTo := uint64(1)
			buf := make(map[uint64]*wrap)
			for w := range interCh {
				if w.i == supposedTo {
					outbound <- w.d
					for ; buf[supposedTo+1] != nil; supposedTo++ {
						outbound <- buf[supposedTo+1].d
						delete(buf, supposedTo+1)
					}
					supposedTo++
				} else {
					buf[w.i] = w
				}
			}
		}()
		if degree <= 0 {
			go func() {
				wg := sync.WaitGroup{}
				defer close(interCh)
				defer wg.Wait()
				i := uint64(0)
				for d := range inbound {
					i++
					wg.Add(1)
					go func(i uint64, d I) {
						defer wg.Done()
						if o := fn(d); !cfg.DropNil || (nilable && utils.IsNil(o)) {
							interCh <- &wrap{i, o}
						}
					}(i, d)
				}
			}()
		} else {
			go func() {
				wg := sync.WaitGroup{}
				wg2 := make(chan struct{}, degree)
				defer close(interCh)
				defer wg.Wait()
				i := uint64(0)
				for d := range inbound {
					i++
					wg.Add(1)
					wg2 <- struct{}{}
					go func(i uint64, d I) {
						defer wg.Done()
						defer func() { <-wg2 }()
						interCh <- &wrap{i, fn(d)}
					}(i, d)
				}
			}()
		}
		return outbound
	}
}

func MapParallelOrdered2StageFactory[I, O any](fn func(item I) O, degree int) ProcessStage[I, O] {
	cfg := defaultConfig()
	nilable := utils.IsNilable(*new(O))
	return func(inbound Stream[I], opts ...StageOption) Stream[O] {
		for _, opt := range opts {
			opt(cfg)
		}
		outbound := make(chan O, cap(inbound))
		if degree <= 0 {
			degree = runtime.NumCPU()
		}

		type wrapI struct {
			i uint64
			d I
		}
		type wrapO struct {
			i uint64
			d O
		}
		worker := func(channel <-chan *wrapI) <-chan *wrapO {
			out := make(chan *wrapO, cap(channel))
			go func() {
				defer close(out)
				for w := range channel {
					if o := fn(w.d); !cfg.DropNil || (nilable && utils.IsNil(o)) {
						out <- &wrapO{w.i, o}
					}
				}
			}()
			return out
		}

		workerInCh := make(chan *wrapI, cap(inbound)) // send inputs to this channel, workers would read from it
		workerOutChs := make([]<-chan *wrapO, 0)
		// spawn workers
		for range degree {
			workerOutChs = append(workerOutChs, worker(workerInCh))
		}

		// fanin from workers' out channels, order results and send them to (out) channel
		go func() {
			defer close(outbound)
			supposedTo := uint64(1)
			buf := make(map[uint64]*wrapO)
			cases := make([]reflect.SelectCase, len(workerOutChs))
			for i, ch := range workerOutChs {
				cases[i] = reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch),
				}
			}
			remaining := len(cases)
			for remaining > 0 {
				chosen, value, ok := reflect.Select(cases)
				if !ok {
					// channel closed; disable it
					cases[chosen].Chan = reflect.Value{}
					remaining--
					continue
				}
				msg := value.Interface()
				w, ok := msg.(*wrapO)
				if !ok {
					continue
				}
				if w.i == supposedTo {
					outbound <- w.d
					for ; buf[supposedTo+1] != nil; supposedTo++ {
						outbound <- buf[supposedTo+1].d
						delete(buf, supposedTo+1)
					}
					supposedTo++
				} else {
					buf[w.i] = w
				}
			}
		}()

		// main loop reads from inbound and fanout to workers via a single channel (workerInCh)
		go func() {
			defer close(workerInCh)
			i := uint64(0)
			for d := range inbound {
				i++
				workerInCh <- &wrapI{i, d}
			}
		}()
		return outbound
	}
}
