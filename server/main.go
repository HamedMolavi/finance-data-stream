package server

import (
	"net/http"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type Server struct {
	*http.ServeMux
}

type ServerSettings struct {
	Log  bool
	Addr string
}

func NewServer(settings ServerSettings) *Server {
	server := &Server{}
	mux := http.NewServeMux()
	server.ServeMux = mux
	var handler http.Handler = mux
	if settings.Log {
		// Wrap mux with a small logging middleware
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			mux.ServeHTTP(w, r)
			logrus.Infof("%s %s %s", r.Method, r.URL.String(), time.Since(start))
		})
		logrus.Infof("server starting on %s", settings.Addr)
	}
	go func() {
		err := http.ListenAndServe(settings.Addr, handler)
		logrus.Error(err)
		os.Exit(1)
	}()
	return server
}
