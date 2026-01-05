package egress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
)

//////////////
//  CONFIG  //
//////////////

type sinkConfig struct{}

func (c *sinkConfig) Validate(_ *config.AnomalyCollector) {}

/////////////
//  STAGE  //
/////////////

// SinkStage is an egress stage that simply destroys all incoming messages.
// It is intended for testing purposes.
type SinkStage[T msgBody] struct {
	*stageBase[any, T, *sinkConfig]
}

// NewSinkStage returns a new sink egress stage.
func NewSinkStage[T msgBody](inputConnector msgConn[T]) *SinkStage[T] {
	return &SinkStage[T]{
		stageBase: newStageBase[any]("sink", inputConnector, &sinkConfig{}),
	}
}

// Init initializes the sink stage.
func (ss *SinkStage[T]) Init(_ context.Context) error {
	ss.stageBase.init()
	return nil
}

// Run runs the sink stage.
func (ss *SinkStage[T]) Run(ctx context.Context) {
	ss.stageBase.run()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgIn, err := ss.inputConnector.Read(ctx)
		if err != nil {
			// Check if the input connector is closed, if so stop
			if errors.Is(err, connector.ErrClosed) {
				ss.tel.LogInfo("input connector is closed, stopping")
				return
			}

			continue
		}

		msgIn.Destroy()
	}
}

// Close closes the sink stage.
func (ss *SinkStage[T]) Close() {
	ss.stageBase.close()
}
