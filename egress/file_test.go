package egress

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/stretchr/testify/require"
)

type fileTestMsg struct {
	data []byte
}

func (m *fileTestMsg) Destroy() {}

func (m *fileTestMsg) GetBytes() []byte {
	return m.data
}

func Test_FileStage_FlushesBufferedDataAfterIdleDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")

	cfg := NewFileConfig(path)
	cfg.BufferSize = 64
	cfg.FlushThresholdPercentage = 1
	cfg.FlushDeadline = 10 * time.Millisecond

	conn := connector.NewRingBuffer[*fileTestMsg](1)
	stage := NewFileStage(conn, cfg)
	require.NoError(t, stage.Init(t.Context()))

	go stage.Run(t.Context())

	msg := message.NewMessage(&fileTestMsg{data: []byte("hello")})
	msg.SetReceiveTime(time.Now())
	require.NoError(t, conn.Write(msg))

	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		return err == nil && string(data) == "hello"
	}, time.Second, time.Millisecond)

	conn.Close()
	stage.Close(t.Context())
}
