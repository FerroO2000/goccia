package goccia

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type cancellableMockStage struct {
	*mockStage

	runStarted chan struct{}
	runExited  chan struct{}
	// UnixNano, set atomically when Run exits
	canceledAt atomic.Int64
}

func newCancellableMockStage(name string, inputs, outputs []uintptr) *cancellableMockStage {
	return &cancellableMockStage{
		mockStage: newMockStage(name, inputs, outputs),

		runStarted: make(chan struct{}),
		runExited:  make(chan struct{}),
	}
}

func (m *cancellableMockStage) Run(ctx context.Context) {
	close(m.runStarted)
	defer close(m.runExited)

	<-ctx.Done()

	m.canceledAt.Store(time.Now().UnixNano())

	// Fake a job before exiting
	time.Sleep(10 * time.Millisecond)
}

// ─── Pipeline Cancellation Order ────────────────────────────────────────────|

// A → C \          / → G
//         → E → F -
// B → D /          \ → H
//
// Topology:
//
//	Roots:   A, B
//	Linear:  A → C, B → D
//	Fan-in:  C → E, D → E
//	Linear:  E→F
//	Fan-out: F → G, F → H
//	Leaves:  G, H
//
// Shutdown order invariants:
//
//	A and B exit Run before C and D
//	C and D exit Run before E
//	E exits Run before F
//	F exits Run before G and H

func Test_Pipeline_CancellationOrder(t *testing.T) {
	assert := assert.New(t)

	ac := newConn()
	bd := newConn()
	ce := newConn()
	de := newConn()
	ef := newConn()
	fg := newConn()
	fh := newConn()

	stageA := newCancellableMockStage("A", nil, []uintptr{ac})
	stageB := newCancellableMockStage("B", nil, []uintptr{bd})
	stageC := newCancellableMockStage("C", []uintptr{ac}, []uintptr{ce})
	stageD := newCancellableMockStage("D", []uintptr{bd}, []uintptr{de})
	stageE := newCancellableMockStage("E", []uintptr{ce, de}, []uintptr{ef})
	stageF := newCancellableMockStage("F", []uintptr{ef}, []uintptr{fg, fh})
	stageG := newCancellableMockStage("G", []uintptr{fg}, nil)
	stageH := newCancellableMockStage("H", []uintptr{fh}, nil)

	p := NewPipeline()
	p.AddStage(stageA)
	p.AddStage(stageB)
	p.AddStage(stageC)
	p.AddStage(stageD)
	p.AddStage(stageE)
	p.AddStage(stageF)
	p.AddStage(stageG)
	p.AddStage(stageH)

	ctx, cancel := context.WithCancel(t.Context())
	assert.NoError(p.Init(ctx))
	go p.Run(ctx)

	// Wait for all stages to have entered their Run loops
	for _, s := range []*cancellableMockStage{
		stageA, stageB, stageC, stageD, stageE, stageF, stageG, stageH,
	} {
		<-s.runStarted
	}

	// Cancel the pipeline
	cancel()
	p.Close(t.Context())

	canceledBefore := func(a, b *cancellableMockStage) bool {
		return a.canceledAt.Load() <= b.canceledAt.Load()
	}

	// A and B (roots) must exit before their children
	assert.True(canceledBefore(stageA, stageC), "A must exit before C")
	assert.True(canceledBefore(stageB, stageD), "B must exit before D")

	// Fan-in: both C and D must exit before E
	assert.True(canceledBefore(stageC, stageE), "C must exit before E")
	assert.True(canceledBefore(stageD, stageE), "D must exit before E")

	// Linear
	assert.True(canceledBefore(stageE, stageF), "E must exit before F")

	// Fan-out: F must exit before both G and H
	assert.True(canceledBefore(stageF, stageG), "F must exit before G")
	assert.True(canceledBefore(stageF, stageH), "F must exit before H")
}

// ─── Pipeline Close Cancellation ────────────────────────────────────────────|

func Test_Pipeline_CloseCancellation(t *testing.T) {
	assert := assert.New(t)

	ab, bc := newConn(), newConn()

	a := newCancellableMockStage("A", nil, []uintptr{ab})
	b := newCancellableMockStage("B", []uintptr{ab}, []uintptr{bc})
	c := newCancellableMockStage("C", []uintptr{bc}, nil)

	p := NewPipeline()
	p.AddStage(a)
	p.AddStage(b)
	p.AddStage(c)

	ctx, cancel := context.WithCancel(t.Context())

	assert.NoError(p.Init(ctx))
	go p.Run(ctx)

	<-a.runStarted
	<-b.runStarted
	<-c.runStarted

	cancel()
	p.Close(t.Context())

	// If Close() returned, all Run goroutines must have exited
	assert.True(isClosed(a.runExited), "A.Run must have exited")
	assert.True(isClosed(b.runExited), "B.Run must have exited")
	assert.True(isClosed(c.runExited), "C.Run must have exited")
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
