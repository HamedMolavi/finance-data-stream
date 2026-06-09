package server

import (
	"net/http"
)

func (s *Server) EnablePing() *Server {
	if s == nil {
		return s
	}
	s.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	})
	return s
}

func (s *Server) EnableProf() *Server {
	if s == nil {
		return s
	}
	// pprof: note we imported net/http/pprof which registers handlers on DefaultServeMux
	// We want the pprof endpoints under /debug/pprof/, so we need to forward that path to http.DefaultServeMux
	// Create a small proxy handler:
	s.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
		// Serve using DefaultServeMux which has pprof registered.
		http.DefaultServeMux.ServeHTTP(w, r)
	})
	// also handle the other pprof paths (profile, cmdline, symbol, trace)
	s.HandleFunc("/debug/pprof/cmdline", func(w http.ResponseWriter, r *http.Request) { http.DefaultServeMux.ServeHTTP(w, r) })
	s.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) { http.DefaultServeMux.ServeHTTP(w, r) })
	s.HandleFunc("/debug/pprof/symbol", func(w http.ResponseWriter, r *http.Request) { http.DefaultServeMux.ServeHTTP(w, r) })
	s.HandleFunc("/debug/pprof/trace", func(w http.ResponseWriter, r *http.Request) { http.DefaultServeMux.ServeHTTP(w, r) })
	return s
}
