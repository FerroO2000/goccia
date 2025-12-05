// Package goccia provides the main entrypoint for the goccia library.
package goccia

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
)

// StageRunningMode represents the running mode of a stage.
type StageRunningMode = config.StageRunningMode

const (
	// StageRunningModeSingle enforces a single-threaded running mode.
	StageRunningModeSingle = config.StageRunningModeSingle
	// StageRunningModePool enforces a multi-threaded running mode (worker pool).
	StageRunningModePool = config.StageRunningModePool
)

// StageConfig represents the configuration for a stage.
type StageConfig = config.Stage

// Stage defines the interface for a generic stage.
type Stage interface {
	// Init initializes the stage.
	Init(ctx context.Context) error
	// Run runs the stage.
	Run(ctx context.Context)
	// Close closes (forever) the stage.
	Close()
}

// Connector represents the interface for a generic connector
// to be used for connecting the stages.
type Connector[T any] = connector.Connector[T]

// Pipeline represents a generic pipeline.
// It is the entrypoint for the stages.
type Pipeline struct {
	stages []Stage

	wg        *sync.WaitGroup
	isRunning bool
}

// NewPipeline returns a new pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{
		stages: []Stage{},

		wg:        &sync.WaitGroup{},
		isRunning: false,
	}
}

// AddStage adds a stage to the pipeline.
// The order of the stages is important.
func (p *Pipeline) AddStage(stage Stage) {
	if p.isRunning {
		return
	}

	p.stages = append(p.stages, stage)
}

// Init initializes all the stages.
func (p *Pipeline) Init(ctx context.Context) error {
	for _, stage := range p.stages {
		if err := stage.Init(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Run runs all the stages.
// It will spawn a goroutine for each stage.
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

// Close closes all the stages.
// It blocks until all the stages are closed.
func (p *Pipeline) Close() {
	for _, stage := range p.stages {
		stage.Close()
	}

	p.wg.Wait()
}
