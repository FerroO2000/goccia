package processor

import (
	"testing"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_MergeStage(t *testing.T) {
	const (
		connSize         = uint64(32)
		inputCount       = 3
		msgCountPerInput = 4
	)

	inputs := make([]msgConn[*dummyMsg], 0, inputCount)
	expectedMessages := make(map[*msg[*dummyMsg]]struct{}, inputCount*msgCountPerInput)

	for inputIdx := range inputCount {
		in := connector.NewRingBuffer[*dummyMsg](connSize)
		inputs = append(inputs, in)

		for msgIdx := range msgCountPerInput {
			msgIn := message.NewMessage(&dummyMsg{value: inputIdx*msgCountPerInput + msgIdx})
			expectedMessages[msgIn] = struct{}{}

			require.NoError(t, in.Write(msgIn))
		}

		in.Close()
	}

	out := connector.NewRingBuffer[*dummyMsg](connSize)

	cfg := NewMergeConfig()
	cfg.OutputQueueSize = 2

	stage := NewMergeStage(inputs, out, cfg)
	require.NoError(t, stage.Init(t.Context()))

	stage.Run(t.Context())
	stage.Close(t.Context())

	for range inputCount * msgCountPerInput {
		msgOut, err := out.Read(t.Context())
		require.NoError(t, err)

		assert.Contains(t, expectedMessages, msgOut)
		delete(expectedMessages, msgOut)
	}

	_, err := out.Read(t.Context())
	assert.ErrorIs(t, err, connector.ErrClosed)
	assert.Empty(t, expectedMessages)
}

func Test_MergeRunner_InitRequiresInput(t *testing.T) {
	out := connector.NewRingBuffer[*dummyMsg](1)
	runner := newMergeRunner([]msgConn[*dummyMsg]{}, out)
	runner.SetEnvironment(newMergeEnv(NewMergeConfig()))

	require.EqualError(t, runner.Init(t.Context()), "no input connector specified")
}

func Test_MergeConfig_ValidateOutputQueueSize(t *testing.T) {
	tests := map[string]int{
		"zero":     0,
		"negative": -1,
	}

	for name, outputQueueSize := range tests {
		t.Run(name, func(t *testing.T) {
			in := connector.NewRingBuffer[*dummyMsg](1)
			out := connector.NewRingBuffer[*dummyMsg](1)

			cfg := NewMergeConfig()
			cfg.OutputQueueSize = outputQueueSize

			stage := NewMergeStage([]msgConn[*dummyMsg]{in}, out, cfg)
			require.NoError(t, stage.Init(t.Context()))

			assert.Equal(t, DefaultMergeConfigOutputQueueSize, cfg.OutputQueueSize)
		})
	}
}
