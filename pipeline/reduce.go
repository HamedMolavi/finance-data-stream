package pipeline

import "github.com/HamedMolavi/finance-data-stream/utils"

func ReduceStageFactory[I, O any](zero O, fn func(zero O, item I)) ProcessStage[I, O] {
	cfg := defaultConfig()
	nilable := utils.IsNilable(*new(O))
	return func(inbound Stream[I], opts ...StageOption) Stream[O] {
		for _, opt := range opts {
			opt(cfg)
		}
		out := make(chan O, cap(inbound))
		go func() {
			defer close(out)
			defer func() {
				if !cfg.DropNil || (nilable && utils.IsNil(zero)) {
					out <- zero
				}
			}()
			for d := range inbound {
				fn(zero, d)
			}
		}()
		return out
	}
}
