package main

import (
	"context"
	"net/http"

	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type echoHandler struct {
	processor.GenericHandlerBase
}

func newEchoHandler() *echoHandler {
	return &echoHandler{}
}

func (h *echoHandler) Handle(_ context.Context, req *ingress.HTTPMessage) (*egress.HTTPMessage, error) {
	return &egress.HTTPMessage{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":  []string{"application/octet-stream"},
			"X-Echo-Method": []string{req.Method},
			"X-Echo-Path":   []string{req.Path},
		},
		Body: req.Body,
	}, nil
}
