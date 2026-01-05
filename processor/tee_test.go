package processor

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/stretchr/testify/assert"
)

type dummyMsg struct {
	value int
}

func (m *dummyMsg) Destroy() {}

func Test_TeeStage(t *testing.T) {
	assert := assert.New(t)

	connSize := uint32(32)
	outConnCount := 3

	inConn := connector.NewRingBuffer[*dummyMsg](connSize)

	outConnectors := make([]msgConn[*dummyMsg], 0, outConnCount)
	for range outConnCount {
		outConnectors = append(outConnectors, connector.NewRingBuffer[*dummyMsg](connSize))
	}

	stage := NewTeeStage(inConn, outConnectors...)

	assert.NoError(stage.Init(t.Context()))

	msgBody := &dummyMsg{value: 1}
	msgIn := message.NewMessage(msgBody)
	assert.NoError(inConn.Write(msgIn))

	ctx, cancelCtx := context.WithCancel(t.Context())

	msgCountPerOutput := 1
	targetMsgCount := msgCountPerOutput * outConnCount
	var currMsgCount atomic.Int64

	readOutput := func(out msgConn[*dummyMsg]) {
		for range msgCountPerOutput {
			msgOut, err := out.Read(t.Context())
			assert.NoError(err)
			assert.Equal(msgBody, msgOut.GetBody())

			currMsgCount.Add(1)
		}

		if currMsgCount.Load() == int64(targetMsgCount) {
			cancelCtx()
		}
	}

	for _, outConn := range outConnectors {
		go readOutput(outConn)
	}

	stage.Run(ctx)

	inConn.Close()
	stage.Close()
}
