package scaler

import (
	"context"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/internal/config"
)

func Test_Scaler_CloseWaitsForRun(t *testing.T) {
	cfg := config.NewPool()
	cfg.AutoScaleInterval = time.Hour

	scaler := NewScaler(cfg)
	runCtx, cancelRunCtx := context.WithCancel(t.Context())

	go scaler.Run(runCtx)

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		scaler.Close()
	}()

	select {
	case <-closeDone:
		t.Fatal("scaler closed before its run loop exited")
	case <-time.After(10 * time.Millisecond):
	}

	cancelRunCtx()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("scaler close did not return after its run loop exited")
	}
}
