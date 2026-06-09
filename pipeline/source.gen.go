package pipeline

import (
	"context"

	"github.com/HamedMolavi/finance-data-stream/utils"
)

func InsertSourceFactory[I any](inputs ...I) SourceStage[I] {
	nilable := utils.IsNilable(*new(I))
	return func(ctx context.Context, opts ...StageOption) Stream[I] {
		cfg := CreateConfig(opts)
		if cfg.TrySync && len(inputs) < 1024*1024 {
			out := make(chan I, cap(inputs)+1)
			defer close(out)
		loop:
			for _, i := range inputs {
				select {
				case <-ctx.Done():
					break loop
				default:
					if nilable && utils.IsNil(i) {
						out <- i
					}
				}
			}
			return out
		}
		out := make(chan I, 1)
		go func() {
			defer close(out)
			for _, i := range inputs {
				select {
				case <-ctx.Done():
					break
				default:
					if nilable && utils.IsNil(i) {
						out <- i
					}
				}
			}
		}()
		return out
	}
}
