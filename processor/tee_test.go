package processor

import (
	"sync"
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

	connSize := uint64(32)
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

	msgCountPerOutput := 1
	targetMsgCount := int64(msgCountPerOutput * outConnCount)
	var currMsgCount atomic.Int64

	wg := sync.WaitGroup{}
	wg.Add(outConnCount)

	readOutput := func(out msgConn[*dummyMsg]) {
		defer wg.Done()

		for range msgCountPerOutput {
			msgOut, err := out.Read(t.Context())
			assert.NoError(err)
			assert.Equal(msgBody, msgOut.GetBody())

			currMsgCount.Add(1)
		}
	}

	for _, outConn := range outConnectors {
		go readOutput(outConn)
	}

	go stage.Run(t.Context())

	wg.Wait()

	inConn.Close()
	stage.Close()

	assert.Equal(targetMsgCount, currMsgCount.Load())
}
