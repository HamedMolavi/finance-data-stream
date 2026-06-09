package pipeline

import (
	"reflect"
	"sync"
)

type Error struct {
	Error       error
	StageConfig *StageConfig
	Input       any
	Output      any
}

type ErrorStream Stream[*Error]
type ErrorRegistererFunc func(ErrorStream)

type ErrorPipeTag string

const (
	DEFAULT_ERROR_PIPE_TAG ErrorPipeTag = "DEFAULT"
)

type errorPipeST struct {
	inbounds []reflect.SelectCase
	outbound chan *Error
}

type ErrorPipeline struct {
	mu            *sync.RWMutex
	errorPipesMap map[ErrorPipeTag]*errorPipeST
}

///////////////////////////////////////////////
///////////////////////////////////////////////

func NewErrorPipelineFactory() *ErrorPipeline {
	e := &ErrorPipeline{
		errorPipesMap: map[ErrorPipeTag]*errorPipeST{
			DEFAULT_ERROR_PIPE_TAG: {inbounds: make([]reflect.SelectCase, 0, 16), outbound: make(chan *Error, 100)},
		},
	}
	e.loop(e.errorPipesMap[DEFAULT_ERROR_PIPE_TAG])
	return e
}

func (e *ErrorPipeline) DefaultRegisterer() ErrorRegistererFunc {
	return e.RegistererFactory(DEFAULT_ERROR_PIPE_TAG)
}

func (e *ErrorPipeline) RegistererFactory(tag ErrorPipeTag) ErrorRegistererFunc {
	return func(s ErrorStream) {
		e.mu.Lock()
		defer e.mu.Unlock()
		if epipe, ok := e.errorPipesMap[tag]; ok {
			epipe.inbounds = append(epipe.inbounds,
				reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(s),
				},
			)
		} else {
		}
	}
}

func (e *ErrorPipeline) loop(errorPipe *errorPipeST) {
	go func() {
		for {
			e.mu.RLock()
			chosen, value, ok := reflect.Select(errorPipe.inbounds)
			e.mu.RUnlock()
			if !ok {
				// channel closed; disable it
				e.mu.Lock()
				errorPipe.inbounds[chosen].Chan = reflect.Value{}
				e.mu.Unlock()
				continue
			}
			msg := value.Interface()
			errorPipe.outbound <- msg.(*Error)
		}
	}()
}

func (e *ErrorPipeline) DefaultStream() Stream[*Error] {
	return e.Stream(DEFAULT_ERROR_PIPE_TAG)
}
func (e *ErrorPipeline) Stream(tag ErrorPipeTag) Stream[*Error] {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.errorPipesMap[tag].outbound
}
