package pipeline

import "context"

type Stream[T any] <-chan T

func NewStream[T any](ch chan T) Stream[T] { return Stream[T](ch) }

type Done <-chan struct{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type SourceStage[T any] func(context.Context, ...StageOption) Stream[T]

type TrySourceStage[T any] func(context.Context, ErrorRegistererFunc, ...StageOption) Stream[T]

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ProcessStage[I, O any] func(Stream[I], ...StageOption) Stream[O]

type TryProcessStage[I, O any] func(ErrorRegistererFunc, Stream[I], ...StageOption) Stream[O]

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type RoutingTable[K comparable, T any] struct{ routes map[K]chan T }

func NewRoutingTable[K comparable, T any](routes map[K]chan T) *RoutingTable[K, T] {
	return &RoutingTable[K, T]{routes: routes}
}
func (r *RoutingTable[K, T]) Stream(key K) Stream[T] {
	return r.routes[key]
}
func (r *RoutingTable[K, T]) For(yield func(K, Stream[T]) bool) {
	for key, ch := range r.routes {
		if !yield(key, ch) {
			return
		}
	}
}

type RouterSourceStage[K comparable, I any] func(context.Context, ...StageOption) *RoutingTable[K, I]

type RouterStage[K comparable, I any] func(Stream[I], ...StageOption) *RoutingTable[K, I]
type ReRouterStage[KI, KO comparable, I any] func(*RoutingTable[KI, I], ...StageOption) *RoutingTable[KO, I]

type ProcessRouterStage[K comparable, I, O any] func(Stream[I], ...StageOption) *RoutingTable[K, O]
type ProcessReRouterStage[KI comparable, I any, KO comparable, O any] func(*RoutingTable[KI, I], ...StageOption) *RoutingTable[KO, O]

type MergeStage[K comparable, I any] func(*RoutingTable[K, I], ...StageOption) Stream[I]
type ProcessMergeStage[K comparable, I, O any] func(*RoutingTable[K, I], ...StageOption) Stream[O]

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type SinkStage[I, O any] func(Stream[I], ...StageOption) O

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// type

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Use ports with cautious

// type PortID string
// type Ports[I any] struct {
// 	values        map[PortID]any
// 	defaultStream Stream[I]
// }

// func NewPorts[I any](s Stream[I]) *Ports[I] {
// 	return &Ports[I]{
// 		defaultStream: s,
// 	}
// }
// func GetDefaultPort[T any](p *Ports[T]) Stream[T] {
// 	return p.defaultStream
// }
// func SetPort[T any](p *Ports[T], name PortID, s any) {
// 	p.values[name] = s
// }
// func GetPort[T any](p *Ports[T], name PortID) any {
// 	return p.values[name]
// }

// type PortSourceStage[O any] func(context.Context, ...StageOption) *Ports[O]
// type StreamPortStage[I, O any] func(Stream[I], ...StageOption) *Ports[O]
// type PortPortStage[I, O any] func(*Ports[I], ...StageOption) *Ports[O]
