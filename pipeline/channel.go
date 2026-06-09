package pipeline

import (
	"reflect"
	"sync"
)

// OrDone returns a channel that is closed when any of the provided `doneChs`
// channels is closed or receives a value.
//
// Goroutines:
//   - For len(doneChs) == 0: no goroutines are created.
//   - For len(doneChs) == 1: no goroutines are created; the input channel is returned directly.
//   - For len(doneChs) >= 2: at least 1 goroutine is created.
//     In the general case (len > 3), additional goroutines are created recursively.
//     The total number of goroutines grows approximately as O(n/3) due to the
//     recursive fan-in structure.
//
// Behavior:
//   - The function composes a tree of select statements.
//   - Each node waits on up to three `doneChs` plus one recursive branch.
//
// The returned `orDone` channel is closed when ANY of the following occurs:
//  1. Any input channel in `doneChs` is closed.
//  2. Any input channel in `doneChs` receives a value.
//  3. Any recursively constructed OrDone channel closes due to the above conditions.
//
// Once any of these conditions is met, the corresponding select unblocks,
// the goroutine returns, and `orDone` is closed exactly once.
//
// Notes:
//   - This function is typically used for cancellation propagation.
//   - It does not forward values; it only signals completion via channel closure.
//   - The recursive structure ensures bounded select cases while still handling
//     an arbitrary number of input channels.
func OrDone[T any](doneChs ...Stream[T]) Stream[T] {
	switch len(doneChs) {
	case 0:
		return nil
	case 1:
		return doneChs[0]
	}
	orDone := make(chan T)

	go func() {
		defer close(orDone)
		switch len(doneChs) {
		case 2:
			select {
			case <-doneChs[0]:
			case <-doneChs[1]:
			}
		default:
			select {
			case <-doneChs[0]:
			case <-doneChs[1]:
			case <-doneChs[2]:
			case <-OrDone(append(doneChs[3:], orDone)...):
			}
		}
	}()

	return orDone
}

// FanInOnce reads at most one value from each input channel and forwards those
// values to a single output channel.
//
// It spawns exactly 1 goroutine to coordinate the fan-in across all provided
// channels, regardless of their count.
//
// For each input channel, the function performs at most one successful receive.
// After either:
//  1. Receiving a value from a channel, or
//  2. Observing that the channel is closed before any value is received,
//
// that channel is permanently disabled and will not be selected again.
//
// The returned `out` channel is closed once all of the following conditions are met:
//  1. Every input channel has either produced one value or has been observed closed.
//  2. All selected values (at most one per channel) have been forwarded to `out`.
//  3. No selectable cases remain.
//
// This guarantees that `out` will emit at most len(channels) values and can be
// safely ranged over until it is closed.
func FanInOnce[T any](channels ...Stream[T]) Stream[T] {
	out := make(chan T, len(channels))
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	remaining := len(cases)

	go func() {
		defer close(out)
		for remaining > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				// channel closed; disable it
				cases[chosen].Chan = reflect.Value{}
				remaining--
				continue
			}

			msg := value.Interface()
			out <- msg.(T)

			// disable this case so we only read once
			cases[chosen].Chan = reflect.Value{}
			remaining--
		}
	}()
	return out
}

// FanIn multiplexes multiple input channels into a single output channel.
//
// It creates exactly 1 goroutine to perform the fan-in operation, regardless of
// the number of input channels provided.
//
// The function continuously selects over all input channels using reflect.Select.
// When a value is received from any input channel, it is forwarded to the returned
// `out` channel.
//
// The returned `out` channel is closed after all of the following conditions are met:
//  1. Every input channel has been closed.
//  2. All remaining buffered values from those channels have been received and forwarded.
//  3. No further receive operations are possible (i.e., all select cases have been disabled).
//
// Once these conditions hold, the internal goroutine exits and `out` is closed,
// allowing consumers to safely range over it until exhaustion.
func FanIn[T any](channels ...Stream[T]) Stream[T] {
	out := make(chan T, len(channels))
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	remaining := len(cases)
	go func() {
		defer close(out)
		for remaining > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				// channel closed; disable it
				cases[chosen].Chan = reflect.Value{}
				remaining--
				continue
			}

			msg := value.Interface()
			out <- msg.(T)
		}
	}()
	return out
}

func Unfold[T any](channel Stream[Stream[T]]) Stream[T] {
	out := make(chan T, cap(channel))
	go func() {
		defer close(out)
		for c := range channel {
			go func() {
				// res := sync.Once{}
				for i := range c {
					// res.Do(func() {
					out <- i
					// })
				}
			}()
		}
	}()
	return out
}

func UnfoldStreamOfTables[K comparable, T any](channel Stream[*RoutingTable[K, T]]) *RoutingTable[K, T] {
	outTable := make(map[K]chan T)
	onceMap := make(map[K]*sync.Once)
	go func() {
		for rt := range channel {
			for key, inbound := range rt.For {
				outbound, ok := outTable[key]
				if !ok {
					outTable[key] = make(chan T, cap(inbound))
					onceMap[key] = &sync.Once{}
				}
				once := onceMap[key]
				go func() {
					defer once.Do(func() { close(outbound) })
					for d := range inbound {
						outbound <- d
					}
				}()
			}
		}
	}()
	return NewRoutingTable(outTable)
}
