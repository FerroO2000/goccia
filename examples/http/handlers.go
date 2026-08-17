package main

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type echoHandler struct {
	processor.GenericHandlerBase

	count atomic.Int64
}

func newEchoHandler() *echoHandler {
	return &echoHandler{}
}

func (h *echoHandler) Handle(_ context.Context, _ *ingress.HTTPMessage) (*jsonMessage, error) {
	return processor.NewJSONMessage(responseBody{
		EchoCount: int(h.count.Add(1)),
	}), nil
}

type httpResponseHandler struct {
	processor.GenericHandlerBase
}

func newHTTPResponseHandler() *httpResponseHandler {
	return &httpResponseHandler{}
}

func (h *httpResponseHandler) Handle(_ context.Context, jsonEnc *processor.JSONEncodedMessage) (*egress.HTTPMessage, error) {
	return &egress.HTTPMessage{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: jsonEnc.Data,
	}, nil
}
