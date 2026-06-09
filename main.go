package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/HamedMolavi/finance-data-stream/manager"
	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/HamedMolavi/finance-data-stream/server"
	"github.com/sirupsen/logrus"
)

func main() {
	baseCtx, cancel := context.WithCancelCause(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	///

	s := server.NewServer(server.ServerSettings{Log: true, Addr: "localhost:1234"}).
		EnablePing().EnableProf()
	///

	errPipeFactory := pipeline.NewErrorPipelineFactory()
	errRegistererFunc := errPipeFactory.DefaultRegisterer()
	///

	pipeline.LogSinkFactory[*pipeline.Error](logrus.Errorln)(
		errPipeFactory.DefaultStream(),
	)
	///

	m, err := manager.New(baseCtx)
	if err != nil {
		logrus.Errorln(err)
		return
	}
	managerpPipelineStage := m.PipelineStageFactory(baseCtx)

	///

	m.ResolvePipelineStage(
		managerpPipelineStage(
			manager.InputConversionStage(errRegistererFunc, // 4th stage: convert manager input into Manager Source Request
				manager.JobConversionStage( // 3rd stage: convert user request body into manager ready input
					manager.RequestValidationStage( // 2nd stage: validate user request
						pipeline.ServerSourceFactory("/job", s)(baseCtx, pipeline.WithName("server"), pipeline.WithID("server")), // 1st stage: reading input from http server
						pipeline.WithDropNil(), pipeline.WithName("validate user job"), pipeline.WithID("RequestValidationStage"),
					),
					pipeline.WithDropNil(), pipeline.WithName("convert job to input"), pipeline.WithID("JobConversionStage"),
				), pipeline.WithName("convert input to source request"), pipeline.WithID("InputConversionStage"),
			),
		),
	)

	select {
	case <-baseCtx.Done():
	case <-c:
	}
	cancel(errors.New("Exited!"))
	fmt.Println("Exited")
}
