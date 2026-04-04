package main

import (
	"context"

	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type ingressToEgressHandler struct {
	processor.CustomHandlerBase
}

func newIngressToEgressHandler() *ingressToEgressHandler {
	return &ingressToEgressHandler{}
}

func (h *ingressToEgressHandler) Init(_ context.Context) error {
	return nil
}

func (h *ingressToEgressHandler) Handle(_ context.Context, kafkaIngressMsg *ingress.KafkaMessage) (*egress.KafkaMessage, error) {
	kafkaEgressMsg := egress.NewKafkaMessage()

	kafkaEgressMsg.Topic = "return-topic"
	kafkaEgressMsg.Key = kafkaIngressMsg.Key
	kafkaEgressMsg.Value = kafkaIngressMsg.Value

	return kafkaEgressMsg, nil
}

func (h *ingressToEgressHandler) Close() {}
