package stage

import (
	"context"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/rb"
)

type stageInput[T msgBody] interface {
	getWorkerRunnerReader() connector.MessageConnector[T]
	run(ctx context.Context)
}

type baseInput[T msgBody] struct {
	input connector.MessageConnector[T]
}

func newBaseInput[T msgBody](input connector.MessageConnector[T]) *baseInput[T] {
	return &baseInput[T]{
		input: input,
	}
}

func (sb *baseInput[T]) getWorkerRunnerReader() connector.MessageConnector[T] {
	return sb.input
}

func (sb *baseInput[T]) run(_ context.Context) {}

type fanOut[T msgBody] struct {
	input  connector.MessageConnector[T]
	fanOut connector.MessageConnector[T]
}

func newFanOut[T msgBody](input connector.MessageConnector[T], fanOutCapacity uint64) *fanOut[T] {
	return &fanOut[T]{
		input:  input,
		fanOut: rb.NewRingBuffer[*message.Message[T]](fanOutCapacity, rb.BufferKindSPMC),
	}
}

func (fo *fanOut[T]) getWorkerRunnerReader() connector.MessageConnector[T] {
	return fo.fanOut
}

func (fo *fanOut[T]) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		msg, err := fo.input.Read(ctx)
		if err != nil {
			continue
		}

		if err := fo.fanOut.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

type stageOutput[T msgBody] interface {
	getWorkerRunnerWriter() connector.MessageConnector[T]
	run(ctx context.Context)
	close()
}

type baseOutput[T msgBody] struct {
	output connector.MessageConnector[T]
}

func newBaseOutput[T msgBody](output connector.MessageConnector[T]) *baseOutput[T] {
	return &baseOutput[T]{
		output: output,
	}
}

func (sb *baseOutput[T]) getWorkerRunnerWriter() connector.MessageConnector[T] {
	return sb.output
}

func (sb *baseOutput[T]) run(_ context.Context) {}

func (sb *baseOutput[T]) close() {
	sb.output.Close()
}

type fanIn[T msgBody] struct {
	output connector.MessageConnector[T]
	fanIn  connector.MessageConnector[T]
}

func newFanIn[T msgBody](output connector.MessageConnector[T], fanInCapacity uint64) *fanIn[T] {
	return &fanIn[T]{
		output: output,
		fanIn:  rb.NewRingBuffer[*message.Message[T]](fanInCapacity, rb.BufferKindMPSC),
	}
}

func (fi *fanIn[T]) getWorkerRunnerWriter() connector.MessageConnector[T] {
	return fi.fanIn
}

func (fi *fanIn[T]) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		msg, err := fi.fanIn.Read(ctx)
		if err != nil {
			continue
		}

		if err := fi.output.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

func (fi *fanIn[T]) close() {
	fi.fanIn.Close()
}
