package processor

import (
	"context"
	"testing"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ROBRunner_DrainsBufferedMessagesAfterCancellation(t *testing.T) {
	const messageCount = 2

	input := connector.NewRingBuffer[*CannelloniMessage](messageCount)
	output := connector.NewRingBuffer[*CannelloniMessage](messageCount)

	cfg := NewROBConfig()
	cfg.TimeSmootherEnabled = false

	testEnv := newROBEnv(cfg)
	testEnv.SetTelemetry(telemetry.NewTelemetry("", ""))

	runner := newROBRunner(input, output)
	runner.SetEnvironment(testEnv)
	require.NoError(t, runner.Init(t.Context()))

	for seqNum := range messageCount {
		body := NewCannelloniMessage()
		body.SetSequenceNumber(uint8(seqNum))
		require.NoError(t, input.Write(message.NewMessage(body)))
	}
	input.Close()

	ctx, cancelCtx := context.WithCancel(t.Context())
	cancelCtx()

	runner.Run(ctx)
	runner.Close(t.Context())

	assert.Equal(t, uint64(messageCount), output.Len())
}
