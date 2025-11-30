package main

import (
	"context"

	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type fileHandler struct {
	processor.CustomHandlerBase
}

func newFileHandler() *fileHandler {
	return &fileHandler{}
}

// Handle adds the path of the input file to the beginning of each line to be
// written to the output file.
func (h *fileHandler) Handle(_ context.Context, msgIn, msgOut *ingress.FileMessage) error {
	outChunk := []byte{}
	buf := make([]byte, 512)

	from := 0
	to := 0
	tmpSize := 0

	for _, ch := range msgIn.Chunk {
		to++
		tmpSize++

		if ch == '\n' {
			copy(buf, msgIn.Chunk[from:to])
			from = to
			outChunk = append(outChunk, []byte(msgIn.Path+" ")...)
			outChunk = append(outChunk, buf[:tmpSize]...)
			tmpSize = 0
		}
	}

	msgOut.Chunk = outChunk
	msgOut.ChunkSize = len(outChunk)

	return nil
}
