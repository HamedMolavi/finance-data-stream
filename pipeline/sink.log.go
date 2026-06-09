package pipeline

func LogSinkFactory[I any](logger func(args ...any)) SinkStage[I, any] {
	return func(inbound Stream[I], opts ...StageOption) any {
		go func() {
			for i := range inbound {
				logger(i)
			}
		}()
		return nil
	}
}
