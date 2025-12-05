package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/FerroO2000/goccia"
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/examples/telemetry"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
	"github.com/squadracorsepolito/acmelib"
)

const connectorSize = 2048

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancelCtx()

	telemetry.Init(ctx, "can-server-example")

	udpToCannelloni := connector.NewRingBuffer[*ingress.UDPMessage](connectorSize)
	cannelloniToROB := connector.NewRingBuffer[*processor.CannelloniMessage](connectorSize)
	robToCAN := connector.NewRingBuffer[*processor.CannelloniMessage](connectorSize)
	canToCustom := connector.NewRingBuffer[*processor.CANMessage](connectorSize)
	customToQuestDB := connector.NewRingBuffer[*egress.QuestDBMessage](connectorSize)

	udpCfg := ingress.NewUDPConfig()
	udpStage := ingress.NewUDPStage(udpToCannelloni, udpCfg)

	cannelloniCfg := processor.NewCannelloniConfig(goccia.StageRunningModePool)
	cannelloniStage := processor.NewCannelloniDecoderStage(udpToCannelloni, cannelloniToROB, cannelloniCfg)

	robCfg := processor.NewROBConfig()
	robStage := processor.NewROBStage(cannelloniToROB, robToCAN, robCfg)

	canCfg := processor.NewCANConfig(goccia.StageRunningModePool)
	canCfg.Messages = getMessages()
	canStage := processor.NewCANStage(robToCAN, canToCustom, canCfg)

	customCfg := processor.NewCustomConfig(goccia.StageRunningModePool)
	customCfg.Name = "can_to_questdb"
	customCfg.Stage.Pool.MinWorkers = customCfg.Stage.Pool.InitialWorkers
	customStage := processor.NewCustomStage(newCANToQuestDBHandler(), canToCustom, customToQuestDB, customCfg)

	questDBCfg := egress.NewQuestDBConfig(goccia.StageRunningModePool)
	questDBCfg.Stage.Pool.MinWorkers = questDBCfg.Stage.Pool.InitialWorkers
	questDBStage := egress.NewQuestDBStage(customToQuestDB, questDBCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(udpStage)
	pipeline.AddStage(cannelloniStage)
	pipeline.AddStage(robStage)
	pipeline.AddStage(canStage)
	pipeline.AddStage(customStage)
	pipeline.AddStage(questDBStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}

// getMessages returns the acmelib representation of the CAN messages
// sent by the client.
func getMessages() []*acmelib.Message {
	// Define the integer message
	sigType, err := acmelib.NewIntegerSignalType("uint8_t", 8, false)
	if err != nil {
		panic(err)
	}

	intMsg := acmelib.NewMessage("my_integer_message", 1000, 8)
	for i := range 8 {
		sig, err := acmelib.NewStandardSignal(fmt.Sprintf("int_signal_%d", i), sigType)
		if err != nil {
			panic(err)
		}

		if err := intMsg.InsertSignal(sig, i*8); err != nil {
			panic(err)
		}
	}

	// Define the enum message
	sigEnum := acmelib.NewSignalEnum("enum")
	for i := range 4 {
		_, err := sigEnum.AddValue(i, fmt.Sprintf("value_%d", i))
		if err != nil {
			panic(err)
		}
	}

	enumMsg := acmelib.NewMessage("my_enum_message", 2000, 4)
	for i := range 4 {
		sig, err := acmelib.NewEnumSignal(fmt.Sprintf("enum_signal_%d", i), sigEnum)
		if err != nil {
			panic(err)
		}

		if err := enumMsg.InsertSignal(sig, i*8); err != nil {
			panic(err)
		}
	}

	return []*acmelib.Message{intMsg, enumMsg}
}
