package goccia

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage"
)

type StageRunningMode = stage.RunningMode

const (
	StageRunningModeSingle = stage.RunningModeSingle
	StageRunningModePool   = stage.RunningModePool
)

type Stage interface {
	Init(ctx context.Context) error
	Run(ctx context.Context)
	Close()
}

type Connector[T any] = connector.Connector[T]

type Pipeline struct {
	stages []Stage

	wg        *sync.WaitGroup
	isRunning bool
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		stages: []Stage{},

		wg:        &sync.WaitGroup{},
		isRunning: false,
	}
}

func (p *Pipeline) AddStage(stage Stage) {
	if p.isRunning {
		return
	}

	p.stages = append(p.stages, stage)
}

func (p *Pipeline) Init(ctx context.Context) error {
	for _, stage := range p.stages {
		if err := stage.Init(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) Run(ctx context.Context) {
	p.isRunning = true

	p.wg.Add(len(p.stages))

	for _, stage := range p.stages {
		go func() {
			stage.Run(ctx)
			p.wg.Done()
		}()
	}
}

func (p *Pipeline) Close() {
	for _, stage := range p.stages {
		stage.Close()
	}

	p.wg.Wait()
}
