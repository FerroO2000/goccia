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

func Test_FileStage_RotatesFileAfterInterval(t *testing.T) {
	t.Chdir(t.TempDir())

	cfg := NewFileConfig("out-20060102-150405.000000000.txt")
	cfg.BufferSize = 64
	cfg.FlushThresholdPercentage = 1
	cfg.FlushDeadline = 10 * time.Millisecond
	cfg.RotationEnable = true
	cfg.RotationInterval = 20 * time.Millisecond

	conn := connector.NewRingBuffer[*fileTestMsg](2)
	stage := NewFileStage(conn, cfg)
	require.NoError(t, stage.Init(t.Context()))

	go stage.Run(t.Context())

	msg := message.NewMessage(&fileTestMsg{data: []byte("first")})
	msg.SetReceiveTime(time.Now())
	require.NoError(t, conn.Write(msg))

	require.Eventually(t, func() bool {
		return fileStageTestFileWithContent(t, "out-*.txt", "first") != ""
	}, time.Second, time.Millisecond)

	time.Sleep(2 * cfg.RotationInterval)

	msg = message.NewMessage(&fileTestMsg{data: []byte("second")})
	msg.SetReceiveTime(time.Now())
	require.NoError(t, conn.Write(msg))

	require.Eventually(t, func() bool {
		return fileStageTestFileWithContent(t, "out-*.txt", "second") != ""
	}, time.Second, time.Millisecond)

	firstPath := fileStageTestFileWithContent(t, "out-*.txt", "first")
	secondPath := fileStageTestFileWithContent(t, "out-*.txt", "second")
	require.NotEmpty(t, firstPath)
	require.NotEmpty(t, secondPath)
	require.NotEqual(t, firstPath, secondPath)

	conn.Close()
	stage.Close(t.Context())
}

func Test_FileStage_DoesNotRotateWhenRotationDisabled(t *testing.T) {
	t.Chdir(t.TempDir())

	path := "out-20060102-150405.000000000.txt"
	cfg := NewFileConfig(path)
	cfg.BufferSize = 64
	cfg.FlushThresholdPercentage = 1
	cfg.FlushDeadline = 10 * time.Millisecond
	cfg.RotationInterval = 20 * time.Millisecond

	conn := connector.NewRingBuffer[*fileTestMsg](2)
	stage := NewFileStage(conn, cfg)
	require.NoError(t, stage.Init(t.Context()))

	go stage.Run(t.Context())

	msg := message.NewMessage(&fileTestMsg{data: []byte("first")})
	msg.SetReceiveTime(time.Now())
	require.NoError(t, conn.Write(msg))

	time.Sleep(2 * cfg.RotationInterval)

	msg = message.NewMessage(&fileTestMsg{data: []byte("second")})
	msg.SetReceiveTime(time.Now())
	require.NoError(t, conn.Write(msg))

	conn.Close()
	stage.Close(t.Context())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "firstsecond", string(data))

	matches, err := filepath.Glob("out-*.txt")
	require.NoError(t, err)
	require.Equal(t, []string{path}, matches)
}

func fileStageTestFileWithContent(t *testing.T, pattern, expected string) string {
	t.Helper()

	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)

	for _, path := range matches {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		if string(data) == expected {
			return path
		}
	}

	return ""
}
