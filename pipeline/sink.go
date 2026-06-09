package pipeline

func GetSinkFactory[I any](n int) SinkStage[I, []I] {
	return func(inbound Stream[I], opts ...StageOption) []I {
		out := make([]I, 0, n)
		for i := range inbound {
			out = append(out, i)
		}
		return out
	}
}

func LastSink[I any](inbound Stream[I], opts ...StageOption) I {
	var last I
	for l, ok := <-inbound; ok; l, ok = <-inbound {
		last = l
	}
	return last
}
