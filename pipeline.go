// Package goccia provides the main entrypoint for the goccia library.
package goccia

import (
	"context"
	"sync"
)

type pipelineState = uint8

const (
	pipelineStateIdle pipelineState = iota
	pipelineStateInitialized
	pipelineStateRunning
	pipelineStateClosed
)

// Pipeline represents a generic pipeline.
// It is the entrypoint for the stages.
type Pipeline struct {
	stages     []Stage
	stageGraph *stageGraph

	state    pipelineState
	stateMux *sync.Mutex

	runWg            *sync.WaitGroup
	cancelRunRootCtx context.CancelFunc
}

// NewPipeline returns a new Pipeline instance.
func NewPipeline() *Pipeline {
	return &Pipeline{
		stages:     []Stage{},
		stageGraph: newStageGraph(),

		state:    pipelineStateIdle,
		stateMux: &sync.Mutex{},

		runWg: &sync.WaitGroup{},
	}
}

// AddStage adds a stage to the pipeline.
// The order of the stages is not important,
// since a stage graph is built during initialization.
func (p *Pipeline) AddStage(stage Stage) {
	p.stateMux.Lock()
	defer p.stateMux.Unlock()

	if p.state != pipelineStateIdle {
		return
	}

	p.stages = append(p.stages, stage)
}

// Init initializes all the stages.
func (p *Pipeline) Init(ctx context.Context) error {
	p.stateMux.Lock()
	defer p.stateMux.Unlock()

	if p.state != pipelineStateIdle {
		return nil
	}

	// Build the stage graph and init all the stages in-order
	if err := p.stageGraph.build(p.stages); err != nil {
		return err
	}

	for nodeStage := range p.stageGraph.traverse() {
		if err := nodeStage.stage.Init(ctx); err != nil {
			return err
		}
	}

	p.state = pipelineStateInitialized

	return nil
}

// Run runs all the stages in at least one goroutine
// (Processor and Egress stages has an additional one for cancellation).
//
// This function blocks until all stages Run goroutines exit.
func (p *Pipeline) Run(ctx context.Context) {
	p.stateMux.Lock()

	if p.state != pipelineStateInitialized {
		p.stateMux.Unlock()
		return
	}

	runRootCtx, cancelRunRootCtx := context.WithCancel(ctx)
	p.cancelRunRootCtx = cancelRunRootCtx

	// Create a detached context to be able to cancel stages
	// that are not of kind Ingress
	detachedCtx := context.WithoutCancel(ctx)

	p.runWg.Add(len(p.stages))
	for stageNode := range p.stageGraph.traverse() {
		runCtx := runRootCtx

		if !stageNode.isIngress {
			// All stages that are not of kind Ingress
			// shall use the cancellable detached context
			tmpRunCtx, cancelRunCtx := context.WithCancel(detachedCtx)
			runCtx = tmpRunCtx
			stageNode.cancelRunCtx = cancelRunCtx
		}

		go p.runStage(runCtx, stageNode)
	}

	p.state = pipelineStateRunning
	p.stateMux.Unlock()

	p.runWg.Wait()
}

func (p *Pipeline) runStage(ctx context.Context, stageNode *stageNode) {
	defer p.runWg.Done()

	if !stageNode.isIngress {
		// Processor and Egress stages context should be canceled
		// when the parent stages have exited the run loop
		go func() {
			for parentStageNode := range stageNode.parents() {
				<-parentStageNode.runDoneCh
			}
			stageNode.cancelRun()
		}()
	}

	stageNode.stage.Run(ctx)
	stageNode.markRunDone()
}

// Close closes all the stages.
// It blocks until all the stages are closed.
func (p *Pipeline) Close(ctx context.Context) {
	p.stateMux.Lock()
	defer p.stateMux.Unlock()

	if p.state != pipelineStateRunning {
		return
	}

	p.cancelRunRootCtx()

	for stageNode := range p.stageGraph.traverse() {
		// Only close the stages when they exited the run loop
		<-stageNode.runDoneCh
		stageNode.stage.Close(ctx)
	}

	p.state = pipelineStateClosed
}
