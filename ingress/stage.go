package ingress

import (
	"context"

	"github.com/FerroO2000/goccia/internal"
)

type source[Out msgEnv] interface {
	setTelemetry(tel *internal.Telemetry)
	run(ctx context.Context, outputConnector msgConn[Out])
}

type stage[Out msgEnv] struct {
	tel *internal.Telemetry

	source source[Out]

	outputConnector msgConn[Out]
}

func newStage[Out msgEnv](name string, source source[Out], outConn msgConn[Out]) *stage[Out] {
	tel := internal.NewTelemetry("ingress", name)
	source.setTelemetry(tel)

	return &stage[Out]{
		tel: tel,

		source: source,

		outputConnector: outConn,
	}
}

func (s *stage[Out]) Init(_ context.Context) error {
	s.tel.LogInfo("initializing")

	return nil
}

func (s *stage[Out]) Run(ctx context.Context) {
	s.source.run(ctx, s.outputConnector)
}

func (s *stage[Out]) Close() {
	s.tel.LogInfo("closing")

	// Close the output connector
	s.outputConnector.Close()
}
