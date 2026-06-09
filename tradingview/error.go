package tradingview

import "github.com/HamedMolavi/finance-data-stream/pipeline"

const UNSUPPORTED_RESOLUTION_ERROR string = "unsupported resolution"
const BAD_AUTH_TOKEN_ERROR string = "bad auth token"

var KNOWN_STOP_ERRORS = []string{
	"unsupported resolution",
	"critical_error",
	"invalid symbol",
	"symbol_error",
	"series_error",
}

var KNOWN_RECONNECT_ERRORS = []string{
	"error",
	"unsupported method", // critical_error
}

func NewErrorPipeline() *pipeline.ErrorPipeline {
	return pipeline.NewErrorPipelineFactory()
}
