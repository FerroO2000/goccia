package main

import (
	"context"
	"fmt"

	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type printTickHandler struct {
	processor.GenericHandlerBase

	logMsg string
}

func newPrintTickHandler(label string) *printTickHandler {
	return &printTickHandler{
		logMsg: fmt.Sprintf("received %s tick", label),
	}
}

func (h *printTickHandler) Handle(_ context.Context, tickerMsg *ingress.TickerMessage) (*ingress.TickerMessage, error) {
	h.Telemetry.LogInfo(h.logMsg, "tick_number", tickerMsg.TickNumber)

	msgOut := ingress.NewTickerMessage()
	msgOut.TickNumber = tickerMsg.TickNumber

	return msgOut, nil
}
