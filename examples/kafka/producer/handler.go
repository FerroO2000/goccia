package main

import (
	"context"
	"strconv"

	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type tickerToKafkaHandler struct {
	processor.CustomHandlerBase
}

func newTickerToKafkaHandler() *tickerToKafkaHandler {
	return &tickerToKafkaHandler{}
}

func (h *tickerToKafkaHandler) Init(_ context.Context) error {
	return nil
}

func (h *tickerToKafkaHandler) Handle(_ context.Context, tickerMsg *ingress.TickerMessage, kafkaMsg *egress.KafkaMessage) error {
	tick := tickerMsg.TickNumber
	strTick := strconv.Itoa(tick)

	kafkaMsg.Topic = "example-topic"
	kafkaMsg.Key = []byte(strTick)
	kafkaMsg.Value = []byte(strTick)

	return nil
}

func (h *tickerToKafkaHandler) Close() {}
