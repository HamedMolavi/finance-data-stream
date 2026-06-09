package pipeline

import "github.com/HamedMolavi/finance-data-stream/utils"

func NestedMapFactory[I, O any](fn func(item I) O) ProcessStage[Stream[I], O] {
	nilable := utils.IsNilable(*new(O))
	return func(s Stream[Stream[I]], so ...StageOption) Stream[O] {
		cfg := CreateConfig(so)
		outbound := make(chan O, cap(s))
		go func() {
			defer close(outbound)
			for inbound := range s {

				//
				go func() {
					for in := range inbound {
						if o := fn(in); !cfg.DropNil || (nilable && utils.IsNil(o)) {
							outbound <- o
						}
					}
				}()

			}
		}()
		return outbound
	}
}

func NestedReduceFactory[I, O any](fn func(zero *O, item I)) ProcessStage[Stream[I], *O] {
	nilable := utils.IsNilable(*new(O))
	return func(s Stream[Stream[I]], so ...StageOption) Stream[*O] {
		cfg := CreateConfig(so)
		outbound := make(chan *O, cap(s))
		go func() {
			defer close(outbound)
			for inbound := range s {

				//
				go func() {
					zero := new(O)
					defer func() {
						if !cfg.DropNil || (nilable && utils.IsNil(zero)) {
							outbound <- zero
						}
					}()
					for d := range inbound {
						fn(zero, d)
					}
				}()

			}
		}()
		return outbound
	}
}
