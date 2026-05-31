package egress

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/telemetry"
	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

type questDBTestSender struct {
	atCtxErr         error
	closeCtxErr      error
	closeHasDeadline bool
}

func (s *questDBTestSender) Table(_ string) qdb.LineSender {
	return s
}

func (s *questDBTestSender) Symbol(_, _ string) qdb.LineSender {
	return s
}

func (s *questDBTestSender) Int64Column(_ string, _ int64) qdb.LineSender {
	return s
}

func (s *questDBTestSender) Long256Column(_ string, _ *big.Int) qdb.LineSender {
	return s
}

func (s *questDBTestSender) TimestampColumn(_ string, _ time.Time) qdb.LineSender {
	return s
}

func (s *questDBTestSender) Float64Column(_ string, _ float64) qdb.LineSender {
	return s
}

func (s *questDBTestSender) StringColumn(_, _ string) qdb.LineSender {
	return s
}

func (s *questDBTestSender) BoolColumn(_ string, _ bool) qdb.LineSender {
	return s
}

func (s *questDBTestSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

func (s *questDBTestSender) At(ctx context.Context, _ time.Time) error {
	s.atCtxErr = ctx.Err()
	return s.atCtxErr
}

func (s *questDBTestSender) Flush(_ context.Context) error {
	return nil
}

func (s *questDBTestSender) Close(ctx context.Context) error {
	s.closeCtxErr = ctx.Err()
	_, s.closeHasDeadline = ctx.Deadline()
	return s.closeCtxErr
}

func newQuestDBTestWorker(sender qdb.LineSender) *questDBWorker {
	env := newQuestDBEnv(NewQuestDBConfig(config.StageRunningModeSingle))
	env.SetTelemetry(telemetry.NewTelemetry("", ""))

	worker := &questDBWorker{sender: sender}
	worker.SetEnvironment(env)

	return worker
}

func Test_QuestDBWorker_DeliverWithCanceledContext(t *testing.T) {
	sender := &questDBTestSender{}
	worker := newQuestDBTestWorker(sender)

	qdbMsg := NewQuestDBMessage()
	row := NewQuestDBRow("test")
	row.AddColumn(NewQuestDBIntColumn("value", 1))
	qdbMsg.AddRow(row)

	ctx, cancelCtx := context.WithCancel(t.Context())
	cancelCtx()

	err := worker.Deliver(ctx, message.NewMessage(qdbMsg))

	assert.NoError(t, err)
	assert.NoError(t, sender.atCtxErr)
}

func Test_QuestDBWorker_ClosePreservesActiveDeadline(t *testing.T) {
	sender := &questDBTestSender{}
	worker := newQuestDBTestWorker(sender)

	ctx, cancelCtx := context.WithTimeout(t.Context(), time.Minute)
	defer cancelCtx()

	err := worker.Close(ctx)

	assert.NoError(t, err)
	assert.True(t, sender.closeHasDeadline)
}
