package egress

import (
	"sync/atomic"
	"testing"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/stretchr/testify/assert"
)

type sinkTestMsg struct {
	call func()
}

func (m *sinkTestMsg) Destroy() {
	m.call()
}

func Test_SinkStage(t *testing.T) {
	assert := assert.New(t)

	msgCount := int64(32)
	conn := connector.NewRingBuffer[*sinkTestMsg](uint64(msgCount))

	var destroyCount atomic.Int64
	call := func() {
		destroyCount.Add(1)
	}

	for range msgCount {
		msg := message.NewMessage(&sinkTestMsg{
			call: call,
		})

		assert.NoError(conn.Write(msg))
	}

	stage := NewSinkStage(conn)
	assert.NoError(stage.Init(t.Context()))

	go stage.Run(t.Context())

	conn.Close()
	stage.Close(t.Context())

	assert.Equal(msgCount, destroyCount.Load())
}
