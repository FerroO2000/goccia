package stage

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
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

const runnerPoolTestTimeout = 5 * time.Second

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
		case <-time.After(runnerPoolTestTimeout):
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
	case <-time.After(runnerPoolTestTimeout):
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

type runnerPoolPassThroughWorker struct {
	worker.BaseWorker[*runnerPoolTestEnv]

	started chan<- struct{}
	release <-chan struct{}
	closed  chan<- error
}

func (w *runnerPoolPassThroughWorker) Handle(
	_ context.Context, msgIn *message.Message[*runnerPoolTestMessage],
) (*message.Message[*runnerPoolTestMessage], error) {
	if w.started != nil {
		w.started <- struct{}{}
	}
	if w.release != nil {
		<-w.release
	}

	return message.NewMessage(&runnerPoolTestMessage{
		value: msgIn.GetBody().value,
	}), nil
}

func (w *runnerPoolPassThroughWorker) Close(ctx context.Context) error {
	if w.closed != nil {
		w.closed <- ctx.Err()
	}
	return nil
}

type runnerPoolEgressWorker struct {
	worker.BaseWorker[*runnerPoolTestEnv]

	started   chan<- struct{}
	release   <-chan struct{}
	delivered chan<- int
	closed    chan<- error
}

func (w *runnerPoolEgressWorker) Deliver(
	_ context.Context, msgIn *message.Message[*runnerPoolTestMessage],
) error {
	if w.started != nil {
		w.started <- struct{}{}
	}
	if w.release != nil {
		<-w.release
	}

	w.delivered <- msgIn.GetBody().value
	return nil
}

func (w *runnerPoolEgressWorker) Close(ctx context.Context) error {
	w.closed <- ctx.Err()
	return nil
}

type runnerPoolDelayedInput struct {
	items []*message.Message[*runnerPoolTestMessage]

	readCount         atomic.Int32
	secondReadStarted chan struct{}
	releaseSecondRead <-chan struct{}
}

func (c *runnerPoolDelayedInput) Write(_ *message.Message[*runnerPoolTestMessage]) error {
	return connector.ErrClosed
}

func (c *runnerPoolDelayedInput) Read(_ context.Context) (*message.Message[*runnerPoolTestMessage], error) {
	readIdx := int(c.readCount.Add(1)) - 1
	if readIdx >= len(c.items) {
		return nil, connector.ErrClosed
	}

	if readIdx == 1 {
		close(c.secondReadStarted)
		<-c.releaseSecondRead
	}

	return c.items[readIdx], nil
}

func (*runnerPoolDelayedInput) Close() {}

type runnerPoolBlockingOutput struct {
	release <-chan struct{}

	writeStarted     chan struct{}
	writeStartedOnce sync.Once

	mux    sync.Mutex
	values []int
}

func (c *runnerPoolBlockingOutput) Write(msg *message.Message[*runnerPoolTestMessage]) error {
	c.writeStartedOnce.Do(func() {
		close(c.writeStarted)
	})
	<-c.release

	c.mux.Lock()
	c.values = append(c.values, msg.GetBody().value)
	c.mux.Unlock()

	msg.Destroy()
	return nil
}

func (*runnerPoolBlockingOutput) Read(context.Context) (*message.Message[*runnerPoolTestMessage], error) {
	return nil, connector.ErrClosed
}

func (*runnerPoolBlockingOutput) Close() {}

func (c *runnerPoolBlockingOutput) getValues() []int {
	c.mux.Lock()
	defer c.mux.Unlock()

	return append([]int(nil), c.values...)
}

func newRunnerPoolTestConfig(workerCount, inputQueueSize, outputQueueSize int) (*config.Pool, *config.Stage) {
	poolCfg := config.NewPool()
	poolCfg.AutoScaleEnabled = false
	poolCfg.MinWorkers = workerCount
	poolCfg.MaxWorkers = workerCount
	poolCfg.InitialWorkers = workerCount
	poolCfg.InputQueueSize = inputQueueSize
	poolCfg.OutputQueueSize = outputQueueSize

	return poolCfg, &config.Stage{
		RunningMode: config.StageRunningModePool,
		Pool:        poolCfg,
	}
}

func newRunnerPoolTestEnv() *runnerPoolTestEnv {
	testEnv := env.NewProcessorEnv(config.NewEmpty(), metrics.NewEmptyMetrics())
	testEnv.SetTelemetry(telemetry.NewTelemetry("", ""))
	return testEnv
}

func newRunnerPoolEgressTestEnv(t *testing.T) *runnerPoolTestEnv {
	t.Helper()

	testEnv := env.NewEgressEnv(config.NewEmpty(), metrics.NewEmptyMetrics())
	testEnv.SetTelemetry(telemetry.NewTelemetry("", ""))
	require.NoError(t, testEnv.Init(t.Context()))
	return testEnv
}

func waitForRunnerTestDone(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(runnerPoolTestTimeout):
		t.Fatal(message)
	}
}

func Test_RunnerSingle_CancelUnblocksEmptyRead(t *testing.T) {
	stageCfg := config.NewStage(config.StageRunningModeSingle)
	input := connector.NewRingBuffer[*runnerPoolTestMessage](1)
	output := connector.NewRingBuffer[*runnerPoolTestMessage](1)

	factory := newProcessorWorkerRunnerFactory(
		newInput(input, stageCfg),
		newOutput(output, stageCfg),
		func() *runnerPoolPassThroughWorker {
			return &runnerPoolPassThroughWorker{}
		},
	)
	runner := newRunnerSingle(factory)
	runner.SetEnvironment(newRunnerPoolTestEnv())
	require.NoError(t, runner.Init(t.Context()))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	cancelRunCtx()
	waitForRunnerTestDone(t, runDone, "single runner did not stop after its empty read was canceled")
	runner.Close(t.Context())
}

func Test_RunnerSingle_CancelDrainsBufferedInput(t *testing.T) {
	const messageCount = 32

	stageCfg := config.NewStage(config.StageRunningModeSingle)
	input := connector.NewRingBuffer[*runnerPoolTestMessage](messageCount)
	output := connector.NewRingBuffer[*runnerPoolTestMessage](messageCount)
	for value := range messageCount {
		require.NoError(t, input.Write(message.NewMessage(&runnerPoolTestMessage{value: value})))
	}

	factory := newProcessorWorkerRunnerFactory(
		newInput(input, stageCfg),
		newOutput(output, stageCfg),
		func() *runnerPoolPassThroughWorker {
			return &runnerPoolPassThroughWorker{}
		},
	)
	runner := newRunnerSingle(factory)
	runner.SetEnvironment(newRunnerPoolTestEnv())
	require.NoError(t, runner.Init(t.Context()))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	cancelRunCtx()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	waitForRunnerTestDone(t, runDone, "single runner did not drain buffered input after cancellation")

	for value := range messageCount {
		msgOut, err := output.Read(t.Context())
		require.NoError(t, err)
		assert.Equal(t, value, msgOut.GetBody().value)
		msgOut.Destroy()
	}

	runner.Close(t.Context())
}

func Test_RunnerSingle_CancelWaitsForDownstreamBackpressure(t *testing.T) {
	stageCfg := config.NewStage(config.StageRunningModeSingle)
	input := connector.NewRingBuffer[*runnerPoolTestMessage](1)
	require.NoError(t, input.Write(message.NewMessage(&runnerPoolTestMessage{value: 0})))

	releaseOutput := make(chan struct{})
	output := &runnerPoolBlockingOutput{
		release:      releaseOutput,
		writeStarted: make(chan struct{}),
	}

	factory := newProcessorWorkerRunnerFactory(
		newInput(input, stageCfg),
		newOutput[*runnerPoolTestMessage](output, stageCfg),
		func() *runnerPoolPassThroughWorker {
			return &runnerPoolPassThroughWorker{}
		},
	)
	runner := newRunnerSingle(factory)
	runner.SetEnvironment(newRunnerPoolTestEnv())
	require.NoError(t, runner.Init(t.Context()))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	waitForRunnerTestDone(t, output.writeStarted, "single runner did not reach downstream backpressure")
	cancelRunCtx()

	select {
	case <-runDone:
		t.Fatal("single runner exited while its output write was blocked")
	default:
	}

	close(releaseOutput)
	waitForRunnerTestDone(t, runDone, "single runner did not stop after downstream backpressure was released")
	assertRunnerPoolTestValueSlice(t, output.getValues(), 1)
	runner.Close(t.Context())
}

func Test_RunnerPool_CancelDoesNotStopWorkersBeforeFanOutCloses(t *testing.T) {
	poolCfg, stageCfg := newRunnerPoolTestConfig(1, 1, 1)

	releaseSecondRead := make(chan struct{})
	input := &runnerPoolDelayedInput{
		items: []*message.Message[*runnerPoolTestMessage]{
			message.NewMessage(&runnerPoolTestMessage{value: 1}),
			message.NewMessage(&runnerPoolTestMessage{value: 2}),
		},
		secondReadStarted: make(chan struct{}),
		releaseSecondRead: releaseSecondRead,
	}
	output := connector.NewRingBuffer[*runnerPoolTestMessage](2)

	closed := make(chan error, 1)
	workerMaker := func() *runnerPoolPassThroughWorker {
		return &runnerPoolPassThroughWorker{closed: closed}
	}

	factory := newProcessorWorkerRunnerFactory(
		newInput[*runnerPoolTestMessage](input, stageCfg),
		newOutput(output, stageCfg),
		workerMaker,
	)
	runner := newRunnerPool(factory, poolCfg)
	runner.SetEnvironment(newRunnerPoolTestEnv())
	require.NoError(t, runner.Init(t.Context()))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	waitForRunnerTestDone(t, input.secondReadStarted, "input bridge did not start its delayed read")

	firstMsg, err := output.Read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, firstMsg.GetBody().value)
	firstMsg.Destroy()

	time.Sleep(10 * time.Millisecond)
	cancelRunCtx()

	select {
	case <-closed:
		t.Fatal("worker stopped before the input bridge closed fan-out")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseSecondRead)
	waitForRunnerTestDone(t, runDone, "runner did not drain after the input bridge completed")

	secondMsg, err := output.Read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, secondMsg.GetBody().value)
	secondMsg.Destroy()

	assert.NoError(t, <-closed)
}

func Test_RunnerPool_CancelDrainsOutputsWithDownstreamBackpressure(t *testing.T) {
	const (
		workerCount  = 4
		messageCount = 32
	)

	poolCfg, stageCfg := newRunnerPoolTestConfig(workerCount, 2, 1)

	input := connector.NewRingBuffer[*runnerPoolTestMessage](uint64(messageCount))
	for value := range messageCount {
		require.NoError(t, input.Write(message.NewMessage(&runnerPoolTestMessage{value: value})))
	}
	input.Close()

	releaseOutput := make(chan struct{})
	output := &runnerPoolBlockingOutput{
		release:      releaseOutput,
		writeStarted: make(chan struct{}),
	}

	factory := newProcessorWorkerRunnerFactory(
		newInput(input, stageCfg),
		newOutput[*runnerPoolTestMessage](output, stageCfg),
		func() *runnerPoolPassThroughWorker {
			return &runnerPoolPassThroughWorker{}
		},
	)
	runner := newRunnerPool(factory, poolCfg)
	runner.SetEnvironment(newRunnerPoolTestEnv())
	require.NoError(t, runner.Init(t.Context()))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	waitForRunnerTestDone(t, output.writeStarted, "output bridge did not reach downstream backpressure")
	cancelRunCtx()

	select {
	case <-runDone:
		t.Fatal("runner exited while its output bridge was blocked")
	default:
	}

	close(releaseOutput)
	waitForRunnerTestDone(t, runDone, "runner did not drain after downstream backpressure was released")

	assertRunnerPoolTestValueSlice(t, output.getValues(), messageCount)
}

func Test_RunnerPool_ProcessorCancelDrainStress(t *testing.T) {
	testCases := []struct {
		name            string
		workerCount     int
		inputQueueSize  int
		outputQueueSize int
		messageCount    int
		repetitions     int
	}{
		{name: "single_worker", workerCount: 1, inputQueueSize: 1, outputQueueSize: 1, messageCount: 32, repetitions: 5},
		{name: "tiny_fan_in", workerCount: 4, inputQueueSize: 2, outputQueueSize: 1, messageCount: 128, repetitions: 10},
		{name: "many_workers", workerCount: 8, inputQueueSize: 4, outputQueueSize: 2, messageCount: 256, repetitions: 5},
	}

	for _, tc := range testCases {
		for repetition := range tc.repetitions {
			t.Run(tc.name+"_"+strconv.Itoa(repetition), func(t *testing.T) {
				runRunnerPoolProcessorCancelDrainStress(t, tc.workerCount, tc.inputQueueSize, tc.outputQueueSize, tc.messageCount)
			})
		}
	}
}

func runRunnerPoolProcessorCancelDrainStress(
	t *testing.T, workerCount, inputQueueSize, outputQueueSize, messageCount int,
) {
	t.Helper()

	poolCfg, stageCfg := newRunnerPoolTestConfig(workerCount, inputQueueSize, outputQueueSize)

	input := connector.NewRingBuffer[*runnerPoolTestMessage](uint64(messageCount))
	output := connector.NewRingBuffer[*runnerPoolTestMessage](1)
	for value := range messageCount {
		require.NoError(t, input.Write(message.NewMessage(&runnerPoolTestMessage{value: value})))
	}
	input.Close()

	started := make(chan struct{}, messageCount)
	release := make(chan struct{})
	closed := make(chan error, workerCount)
	factory := newProcessorWorkerRunnerFactory(
		newInput(input, stageCfg),
		newOutput(output, stageCfg),
		func() *runnerPoolPassThroughWorker {
			return &runnerPoolPassThroughWorker{
				started: started,
				release: release,
				closed:  closed,
			}
		},
	)
	runner := newRunnerPool(factory, poolCfg)
	runner.SetEnvironment(newRunnerPoolTestEnv())
	require.NoError(t, runner.Init(t.Context()))

	outputValues := make(chan int, messageCount)
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		for {
			msgOut, err := output.Read(t.Context())
			if err != nil {
				return
			}

			outputValues <- msgOut.GetBody().value
			msgOut.Destroy()
		}
	}()

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	for range workerCount {
		waitForRunnerTestDone(t, started, "worker did not start processing")
	}

	cancelRunCtx()
	close(release)

	waitForRunnerTestDone(t, runDone, "processor runner did not drain")
	waitForRunnerTestDone(t, outputDone, "processor output collector did not stop")

	close(outputValues)
	assertRunnerPoolTestValues(t, outputValues, messageCount)

	for range workerCount {
		assert.NoError(t, <-closed)
	}
}

func Test_RunnerPool_EgressCancelDrainStress(t *testing.T) {
	const (
		workerCount  = 4
		messageCount = 128
	)

	for repetition := range 10 {
		t.Run(strconv.Itoa(repetition), func(t *testing.T) {
			poolCfg, stageCfg := newRunnerPoolTestConfig(workerCount, 2, 1)

			input := connector.NewRingBuffer[*runnerPoolTestMessage](uint64(messageCount))
			for value := range messageCount {
				require.NoError(t, input.Write(message.NewMessage(&runnerPoolTestMessage{value: value})))
			}
			input.Close()

			started := make(chan struct{}, messageCount)
			release := make(chan struct{})
			delivered := make(chan int, messageCount)
			closed := make(chan error, workerCount)
			factory := newEgressWorkerRunnerFactory(
				newInput(input, stageCfg),
				func() *runnerPoolEgressWorker {
					return &runnerPoolEgressWorker{
						started:   started,
						release:   release,
						delivered: delivered,
						closed:    closed,
					}
				},
			)
			runner := newRunnerPool(factory, poolCfg)
			runner.SetEnvironment(newRunnerPoolEgressTestEnv(t))
			require.NoError(t, runner.Init(t.Context()))

			runCtx, cancelRunCtx := context.WithCancel(t.Context())
			runDone := make(chan struct{})
			go func() {
				defer close(runDone)
				runner.Run(runCtx)
			}()

			for range workerCount {
				waitForRunnerTestDone(t, started, "egress worker did not start processing")
			}

			cancelRunCtx()
			close(release)

			waitForRunnerTestDone(t, runDone, "egress runner did not drain")

			close(delivered)
			assertRunnerPoolTestValues(t, delivered, messageCount)

			for range workerCount {
				assert.NoError(t, <-closed)
			}
		})
	}
}

func assertRunnerPoolTestValues(t *testing.T, values <-chan int, messageCount int) {
	t.Helper()

	collectedValues := make([]int, 0, messageCount)
	for value := range values {
		collectedValues = append(collectedValues, value)
	}

	assertRunnerPoolTestValueSlice(t, collectedValues, messageCount)
}

func assertRunnerPoolTestValueSlice(t *testing.T, values []int, messageCount int) {
	t.Helper()

	seen := make([]bool, messageCount)
	for _, value := range values {
		require.GreaterOrEqual(t, value, 0)
		require.Less(t, value, messageCount)
		assert.False(t, seen[value], "duplicate value %d", value)
		seen[value] = true
	}

	for value, wasSeen := range seen {
		assert.True(t, wasSeen, "missing value %d", value)
	}
}
