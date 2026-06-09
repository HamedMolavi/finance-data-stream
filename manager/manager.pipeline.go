package manager

import (
	"context"
	"fmt"

	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/HamedMolavi/finance-data-stream/types"
)

func mergeSourceResult(result *ManagerSourceResult, v *sourceResult) {
	result.Count += v.Count
	result.Retried += v.Retried
	if v.Err != nil {
		result.Errs = append(result.Errs, v.Err)
	}
}

// Second Try
func (m *Manager) PipelineStageFactory(ctx context.Context) pipeline.ProcessStage[*ManagerSourceRequest, *pipeline.RoutingTable[types.Interval, *types.Kline]] {
	return pipeline.MapStageFactory(
		func(srcreq *ManagerSourceRequest) *pipeline.RoutingTable[types.Interval, *types.Kline] {
			generatorStage := pipeline.InsertSourceFactory(srcreq.ForexRequests...)
			forexDispatcherStage := pipeline.MapStageFactory(WouldYouFuckMe)

			return pipeline.UnfoldStreamOfTables(
				forexDispatcherStage(
					generatorStage(ctx, pipeline.WithTrySync()),
				),
			)
		},
	)

}

// pipeline.SinkStage[pipeline.Stream[*pipeline.RoutingTable[types.Interval, *types.Kline]], struct{}]
func (m *Manager) ResolvePipelineStage(inboundStream pipeline.Stream[*pipeline.RoutingTable[types.Interval, *types.Kline]], so ...pipeline.StageOption) struct{} {
	go func() {
		for rt := range inboundStream {
			for interval, inbound := range rt.For {
				go func(interval types.Interval, inbound pipeline.Stream[*types.Kline]) {
					for kline := range inbound {
						fmt.Println(kline)
					}
				}(interval, inbound)
			}
		}
	}()
	return struct{}{}
}

// return pipeline.MapStageFactory(
// 	func(srcreq *ManagerSourceRequest) *ManagerSourceResult {
// 		// forexMapStage := pipeline.MapStageFactory(func(req *ForexSourceRequest) *job[ForexSourceRequest] {
// 		// 	return &job[ForexSourceRequest]{ctx: ctx, request: req, resultCh: make(chan *sourceResult, 1)}
// 		// })
// 		// m.forexDispatcher.NewStage(
// 		// 	pipeline.InsertSourceFactory(srcreq.ForexRequests...)(ctx, pipeline.WithTrySync()),
// 		// )
// 		result := &ManagerSourceResult{ManagerSourceRequest: srcreq, Errs: make([]error, 0)}
// 		return pipeline.LastSink(pipeline.ReduceStageFactory(result, mergeSourceResult)(pipeline.FanInOnce(sourceResultChs...)))
// 	},
// )

// First try
// func (m *Manager) SourceStageFactory(ctx context.Context) pipeline.ProcessStage[*ManagerSourceRequest, *ManagerSourceResult] {
// 	return pipeline.MapStageFactory(
// 		func(srcreq *ManagerSourceRequest) *ManagerSourceResult {
// 			// forexMapStage := pipeline.MapStageFactory(func(req *ForexSourceRequest) *job[ForexSourceRequest] {
// 			// 	return &job[ForexSourceRequest]{ctx: ctx, request: req, resultCh: make(chan *sourceResult, 1)}
// 			// })
// 			// m.forexDispatcher.NewStage(
// 			// 	pipeline.InsertSourceFactory(srcreq.ForexRequests...)(ctx, pipeline.WithTrySync()),
// 			// )
// 			result := &ManagerSourceResult{ManagerSourceRequest: srcreq, Errs: make([]error, 0)}
// 			return pipeline.LastSink(pipeline.ReduceStageFactory(result, mergeSourceResult)(pipeline.FanInOnce(sourceResultChs...)))
// 		},
// 	)
// }
