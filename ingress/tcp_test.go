package ingress

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/stretchr/testify/require"
)

const ingressRunnerTestTimeout = 5 * time.Second

type ingressRunnerBlockingOutput[T msgBody] struct {
	release <-chan struct{}

	writeStarted     chan struct{}
	writeStartedOnce sync.Once
}

func (c *ingressRunnerBlockingOutput[T]) Write(msg *message.Message[T]) error {
	c.writeStartedOnce.Do(func() {
		close(c.writeStarted)
	})
	<-c.release

	msg.Destroy()
	return nil
}

func (*ingressRunnerBlockingOutput[T]) Read(context.Context) (*message.Message[T], error) {
	return nil, connector.ErrClosed
}

func (*ingressRunnerBlockingOutput[T]) Close() {}

func waitForIngressRunnerTestDone(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(ingressRunnerTestTimeout):
		t.Fatal(message)
	}
}

func requireIngressRunnerStillRunning(t *testing.T, runDone <-chan struct{}) {
	t.Helper()

	select {
	case <-runDone:
		t.Fatal("runner exited before its output bridge drained")
	case <-time.After(100 * time.Millisecond):
	}
}

func Test_TCPRunner_CancelWaitsForFanInDrain(t *testing.T) {
	releaseOutput := make(chan struct{})
	output := &ingressRunnerBlockingOutput[*TCPMessage]{
		release:      releaseOutput,
		writeStarted: make(chan struct{}),
	}

	cfg := NewTCPConfig()
	cfg.OutputQueueSize = 1
	testEnv := newTCPEnv(&cfg)
	testEnv.listener = new(net.TCPListener)

	runner := newTCPRunner(output)
	runner.SetEnvironment(testEnv)
	require.NoError(t, runner.Init(t.Context()))
	require.NoError(t, runner.connFanIn.Write(message.NewMessage(NewTCPMessage())))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	cancelRunCtx()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	waitForIngressRunnerTestDone(t, output.writeStarted, "output bridge did not reach downstream backpressure")
	requireIngressRunnerStillRunning(t, runDone)

	close(releaseOutput)
	waitForIngressRunnerTestDone(t, runDone, "runner did not drain after downstream backpressure was released")
	runner.Close(t.Context())
}
