package stage

import (
	"context"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type runnerPoolTestMessage struct {
	value int
}

func (*runnerPoolTestMessage) Destroy() {}

type runnerPoolTestEnv = env.BaseEnv[*config.Empty, *metrics.EmptyMetrics]

type runnerPoolTestWorker struct {
	worker.BaseWorker[*runnerPoolTestEnv]

	started chan<- struct{}
	release <-chan struct{}
	closed  chan<- error
}

func (w *runnerPoolTestWorker) Handle(
	_ context.Context, msgIn *message.Message[*runnerPoolTestMessage],
) (*message.Message[*runnerPoolTestMessage], error) {
	w.started <- struct{}{}
	<-w.release

	return message.NewMessage(&runnerPoolTestMessage{
		value: msgIn.GetBody().value,
	}), nil
}

func (w *runnerPoolTestWorker) Close(ctx context.Context) error {
	w.closed <- ctx.Err()
	return nil
}

func Test_RunnerPool_CloseDrainsBlockedWorkerOutputs(t *testing.T) {
	const workerCount = 2

	poolCfg := config.NewPool()
	poolCfg.AutoScaleEnabled = false
	poolCfg.MinWorkers = workerCount
	poolCfg.MaxWorkers = workerCount
	poolCfg.InitialWorkers = workerCount
	poolCfg.InputQueueSize = workerCount
	poolCfg.OutputQueueSize = 1

	stageCfg := &config.Stage{
		RunningMode: config.StageRunningModePool,
		Pool:        poolCfg,
	}

	input := connector.NewRingBuffer[*runnerPoolTestMessage](workerCount)
	output := connector.NewRingBuffer[*runnerPoolTestMessage](workerCount)

	started := make(chan struct{}, workerCount)
	release := make(chan struct{})
	closed := make(chan error, workerCount)

	workerMaker := func() *runnerPoolTestWorker {
		return &runnerPoolTestWorker{
			started: started,
			release: release,
			closed:  closed,
		}
	}

	factory := newProcessorWorkerRunnerFactory(
		newInput(input, stageCfg),
		newOutput(output, stageCfg),
		workerMaker,
	)
	runner := newRunnerPool(factory, poolCfg)

	testEnv := env.NewProcessorEnv(config.NewEmpty(), metrics.NewEmptyMetrics())
	testEnv.SetTelemetry(telemetry.NewTelemetry("", ""))
	runner.SetEnvironment(testEnv)

	require.NoError(t, runner.Init(t.Context()))

	for value := range workerCount {
		require.NoError(t, input.Write(message.NewMessage(&runnerPoolTestMessage{
			value: value,
		})))
	}

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	for range workerCount {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("worker did not start processing")
		}
	}

	cancelRunCtx()
	input.Close()

	select {
	case <-runDone:
		t.Fatal("runner exited before workers finished draining")
	default:
	}

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		runner.Close(t.Context())
	}()

	close(release)

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("runner close blocked while workers were writing outputs")
	}

	for range workerCount {
		msgOut, err := output.Read(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, msgOut)
	}

	for range workerCount {
		assert.NoError(t, <-closed)
	}
}
