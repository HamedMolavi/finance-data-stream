package manager

import (
	"encoding/json"
	"net/http"

	"github.com/HamedMolavi/finance-data-stream/pipeline"
)

var RequestValidationStage = pipeline.MapStageFactory(
	func(request *pipeline.RequestEnvelope) *JobBody {
		switch request.Raw.Method {
		case http.MethodPost:
			var payload *JobBody
			if err := json.Unmarshal(request.Body, payload); err != nil {
				request.Resp <- &pipeline.Response{Body: []byte("bad request: " + err.Error()), Status: http.StatusBadRequest}
				break
			}
			request.Resp <- &pipeline.Response{Body: []byte("Job is queued!"), Status: http.StatusCreated}
			return payload
		// case http.MethodGet:
		// 	out := manager.Status()
		// 	w.Header().Set("Content-Type", "application/json")
		// 	_ = json.NewEncoder(w).Encode(out)
		default:
			request.Resp <- &pipeline.Response{Body: []byte("method not allowed"), Status: http.StatusMethodNotAllowed}
		}
		return nil
	},
)
