package stage

import (
	"context"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/rb"
)

type stageIO interface {
	getConnectorID() uintptr
	run(ctx context.Context)
}

// ─── Input ──────────────────────────────────────────────────────────────────|

type stageInput[T msgBody] interface {
	stageIO

	getWorkerExecutorReader() connector.MessageConnector[T]
}

func newInput[T msgBody](input msgConn[T], cfg *config.Stage) stageInput[T] {
	switch cfg.RunningMode {
	case config.StageRunningModeSingle:
		return newBaseInput(input)

	case config.StageRunningModePool:
		return newFanOut(input, uint64(cfg.Pool.InputQueueSize))

	default:
		panic("invalid running mode")
	}
}

var _ stageInput[msgBody] = (*baseInput[msgBody])(nil)
var _ stageInput[msgBody] = (*fanOut[msgBody])(nil)

type baseInput[T msgBody] struct {
	input connector.MessageConnector[T]
}

func newBaseInput[T msgBody](input connector.MessageConnector[T]) *baseInput[T] {
	return &baseInput[T]{
		input: input,
	}
}

func (sb *baseInput[T]) getConnectorID() uintptr {
	return connector.GetConnectorID(sb.input)
}

func (sb *baseInput[T]) getWorkerExecutorReader() connector.MessageConnector[T] {
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

func (fo *fanOut[T]) getConnectorID() uintptr {
	return connector.GetConnectorID(fo.input)
}

func (fo *fanOut[T]) getWorkerExecutorReader() connector.MessageConnector[T] {
	return fo.fanOut
}

func (fo *fanOut[T]) run(ctx context.Context) {
	defer fo.fanOut.Close()

	for {
		msg, err := fo.input.Read(ctx)
		if err != nil {
			return
		}

		if err := fo.fanOut.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

// ─── Output ─────────────────────────────────────────────────────────────────|

type stageOutput[T msgBody] interface {
	stageIO

	getWorkerExecutorWriter() connector.MessageConnector[T]
	close()
}

func newOutput[T msgBody](output msgConn[T], cfg *config.Stage) stageOutput[T] {
	switch cfg.RunningMode {
	case config.StageRunningModeSingle:
		return newBaseOutput(output)

	case config.StageRunningModePool:
		return newFanIn(output, uint64(cfg.Pool.OutputQueueSize))

	default:
		panic("invalid running mode")
	}
}

var _ stageOutput[msgBody] = (*baseOutput[msgBody])(nil)
var _ stageOutput[msgBody] = (*fanIn[msgBody])(nil)

type baseOutput[T msgBody] struct {
	output connector.MessageConnector[T]
}

func newBaseOutput[T msgBody](output connector.MessageConnector[T]) *baseOutput[T] {
	return &baseOutput[T]{
		output: output,
	}
}

func (sb *baseOutput[T]) getConnectorID() uintptr {
	return connector.GetConnectorID(sb.output)
}

func (sb *baseOutput[T]) getWorkerExecutorWriter() connector.MessageConnector[T] {
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

func (fi *fanIn[T]) getConnectorID() uintptr {
	return connector.GetConnectorID(fi.output)
}

func (fi *fanIn[T]) getWorkerExecutorWriter() connector.MessageConnector[T] {
	return fi.fanIn
}

func (fi *fanIn[T]) run(ctx context.Context) {
	defer fi.output.Close()

	for {
		msg, err := fi.fanIn.Read(ctx)
		if err != nil {
			return
		}

		if err := fi.output.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

func (fi *fanIn[T]) close() {
	fi.fanIn.Close()
}
