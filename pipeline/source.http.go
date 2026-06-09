package pipeline

import (
	"context"
	"io"
	"net/http"
	"sync"
)

type RequestEnvelope struct {
	Ctx  context.Context
	Body []byte
	Raw  *http.Request
	Resp chan *Response
}
type Response struct {
	Body   []byte
	Status int
}

func ServerSourceFactory(pattern string, s interface {
	HandleFunc(string, func(http.ResponseWriter, *http.Request))
}) SourceStage[*RequestEnvelope] {
	return func(ctx context.Context, opts ...StageOption) Stream[*RequestEnvelope] {
		// cfg := createConfig(opts)
		out := make(chan *RequestEnvelope, 100)
		closeOnce := sync.Once{}
		c := func(w http.ResponseWriter) {
			closeOnce.Do(func() { close(out) })
			http.Error(w, "pipeline closed!", http.StatusServiceUnavailable)
		}
		s.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-ctx.Done():
				c(w)
				return
			default:
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				c(w)
				return
			}
			envelope := &RequestEnvelope{
				Ctx:  r.Context(),
				Body: body,
				Raw:  r,
				Resp: make(chan *Response, 1),
			}
			select {
			case <-ctx.Done():
				c(w)
				return
			case out <- envelope:
			default:
				http.Error(w, "too many request", http.StatusTooManyRequests)
			}
			select {
			case <-ctx.Done():
				c(w)
				return
			case <-r.Context().Done():
				return
			case resp := <-envelope.Resp:
				w.WriteHeader(resp.Status)
				w.Write(resp.Body)
			}
		})
		return out
	}
}
