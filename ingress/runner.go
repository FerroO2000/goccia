package ingress

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/rb"
	"github.com/FerroO2000/goccia/internal/stage/env"
)

// ─── Base ───────────────────────────────────────────────────────────────────|

type runnerBase[E env.Env, T msgBody] struct {
	env E

	outConnector msgConn[T]
	runDone      chan struct{}
}

func newRunnerBase[E env.Env, T msgBody](outConnector msgConn[T]) *runnerBase[E, T] {
	return &runnerBase[E, T]{
		outConnector: outConnector,
		runDone:      make(chan struct{}),
	}
}

func (r *runnerBase[E, T]) notifyRunDone() {
	close(r.runDone)
}

func (r *runnerBase[E, T]) waitRun() {
	<-r.runDone
}

func (r *runnerBase[E, T]) SetEnvironment(env E) {
	r.env = env
}

func (r *runnerBase[E, T]) Init(_ context.Context) error {
	return nil
}

func (r *runnerBase[E, T]) Close(_ context.Context) {
	r.waitRun()
	r.outConnector.Close()
}

func (r *runnerBase[E, T]) Inputs() []uintptr {
	return []uintptr{}
}

func (r *runnerBase[E, T]) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.outConnector)}
}

// ─── Fan-In ─────────────────────────────────────────────────────────────────|

type runnerFanInBase[E env.Env, T msgBody] struct {
	env E

	outConnector msgConn[T]
	runDone      chan struct{}

	fanIn         *rb.RingBuffer[*msg[T]]
	wg            *sync.WaitGroup
	runOutputDone chan struct{}
}

func newRunnerFanInBase[E env.Env, T msgBody](outConnector msgConn[T]) *runnerFanInBase[E, T] {
	return &runnerFanInBase[E, T]{
		outConnector: outConnector,
		runDone:      make(chan struct{}),

		wg: &sync.WaitGroup{},

		runOutputDone: make(chan struct{}),
	}
}

func (r *runnerFanInBase[E, T]) SetEnvironment(env E) {
	r.env = env
}

func (r *runnerFanInBase[E, T]) initFanIn(capacity uint64) {
	r.fanIn = rb.NewRingBuffer[*msg[T]](capacity, rb.BufferKindMPSC)
}

func (r *runnerFanInBase[E, T]) runOutputBridge(ctx context.Context) {
	defer r.outConnector.Close()
	defer close(r.runOutputDone)

	for {
		msg, err := r.fanIn.Read(ctx)
		if err != nil {
			return
		}

		if err := r.outConnector.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

func (r *runnerFanInBase[E, T]) drainAndNotifyRunDone() {
	r.wg.Wait()
	r.fanIn.Close()
	<-r.runOutputDone
	close(r.runDone)
}

func (r *runnerFanInBase[E, T]) addWork() {
	r.wg.Add(1)
}

func (r *runnerFanInBase[E, T]) notifyWorkDone() {
	r.wg.Done()
}

func (r *runnerFanInBase[E, T]) Close(_ context.Context) {
	<-r.runDone
}

func (r *runnerFanInBase[E, T]) Inputs() []uintptr {
	return []uintptr{}
}

func (r *runnerFanInBase[E, T]) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.outConnector)}
}
