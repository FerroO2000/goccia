package ingress

import (
	"context"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
)

type source[Out msgBody] interface {
	setTelemetry(tel *internal.Telemetry)
	run(ctx context.Context, outputConnector msgConn[Out])
}

type stage[Out msgBody, Cfg cfg] struct {
	tel *internal.Telemetry

	cfg Cfg

	source source[Out]

	outputConnector msgConn[Out]
}

func newStage[Out msgBody, Cfg cfg](name string, source source[Out], outConn msgConn[Out], cfg Cfg) *stage[Out, Cfg] {
	tel := internal.NewTelemetry("ingress", name)
	source.setTelemetry(tel)

	return &stage[Out, Cfg]{
		tel: tel,

		cfg: cfg,

		source: source,

		outputConnector: outConn,
	}
}

func (s *stage[Out, Cfg]) Init(_ context.Context) error {
	s.tel.LogInfo("initializing")

	configValidator := config.NewValidator(s.tel)
	configValidator.Validate(s.cfg)

	return nil
}

func (s *stage[Out, Cfg]) Run(ctx context.Context) {
	s.source.run(ctx, s.outputConnector)
}

func (s *stage[Out, Cfg]) Close() {
	s.tel.LogInfo("closing")

	// Close the output connector
	s.outputConnector.Close()
}
