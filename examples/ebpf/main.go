package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/FerroO2000/goccia"
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
)

const connectorSize = 2048

type PingEvent struct {
	SrcIP uint32
	DstIP uint32
	ID    uint16
	Seq   uint16
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancelCtx()

	// Get the network interface
	ifname := "eth0"
	if len(os.Args) > 1 {
		ifname = os.Args[1]
	}

	ebpfToHandler := connector.NewRingBuffer[*ingress.EBPFMessage[PingEvent]](connectorSize)
	handlerToSink := connector.NewRingBuffer[*ingress.EBPFMessage[PingEvent]](connectorSize)

	ebpfConfig := ingress.DefaultEBPFConfig(
		loadBpf,
		func(objs *bpfObjects) (link.Link, error) {
			iface, err := net.InterfaceByName(ifname)
			if err != nil {
				return nil, err
			}
			return link.AttachXDP(link.XDPOptions{
				Program:   objs.PingMonitor,
				Interface: iface.Index,
			})
		},
		func(objs *bpfObjects) *ebpf.Map {
			return objs.PingEvents
		},
	)

	ebpfStage := ingress.NewEBPFStage(ebpfToHandler, ebpfConfig)

	pingHandlerConfig := processor.DefaultCustomConfig(goccia.StageRunningModeSingle)
	pingHandlerConfig.Name = "ping_handler"
	pingHandlerStage := processor.NewCustomStage(newPingHandler(), ebpfToHandler, handlerToSink, pingHandlerConfig)

	sinkStage := egress.NewSinkStage(handlerToSink)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(ebpfStage)
	pipeline.AddStage(pingHandlerStage)
	pipeline.AddStage(sinkStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}
