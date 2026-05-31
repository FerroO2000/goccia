package ingress

import (
	"context"
	"testing"

	"github.com/FerroO2000/goccia/internal/message"
	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/require"
)

func Test_FileRunner_CancelWaitsForFanInDrain(t *testing.T) {
	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)

	releaseOutput := make(chan struct{})
	output := &ingressRunnerBlockingOutput[*FileMessage]{
		release:      releaseOutput,
		writeStarted: make(chan struct{}),
	}

	cfg := NewFileConfig()
	cfg.WatchedDirs = nil
	testEnv := newFileEnv(cfg)
	testEnv.watcher = watcher

	runner := newFileRunner(output)
	runner.SetEnvironment(testEnv)
	require.NoError(t, runner.Init(t.Context()))
	require.NoError(t, runner.fanIn.Write(message.NewMessage(NewFileMessage())))

	runCtx, cancelRunCtx := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runner.Run(runCtx)
	}()

	waitForIngressRunnerTestDone(t, output.writeStarted, "output bridge did not reach downstream backpressure")
	cancelRunCtx()
	requireIngressRunnerStillRunning(t, runDone)

	close(releaseOutput)
	waitForIngressRunnerTestDone(t, runDone, "runner did not drain after downstream backpressure was released")
	runner.Close(t.Context())
}
