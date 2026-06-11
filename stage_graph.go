package goccia

import (
	"context"
	"fmt"
	"iter"
	"weak"
)

type stageNode struct {
	isIngress    bool
	stage        Stage
	parentsWeak  []weak.Pointer[stageNode]
	children     []*stageNode
	runDoneCh    chan struct{}
	cancelRunCtx context.CancelFunc
}

func newStageNode(stage Stage) *stageNode {
	return &stageNode{
		isIngress:   false,
		stage:       stage,
		parentsWeak: []weak.Pointer[stageNode]{},
		children:    []*stageNode{},
		runDoneCh:   make(chan struct{}),
	}
}

func (sn *stageNode) addChild(child *stageNode) {
	sn.children = append(sn.children, child)
	child.parentsWeak = append(child.parentsWeak, weak.Make(sn))
}

func (sn *stageNode) parents() iter.Seq[*stageNode] {
	return func(yield func(*stageNode) bool) {
		for _, wp := range sn.parentsWeak {
			val := wp.Value()
			if val != nil && !yield(val) {
				return
			}
		}
	}
}

func (sn *stageNode) parentCount() int {
	count := 0
	for range sn.parents() {
		count++
	}
	return count
}

func (sn *stageNode) markRunDone() {
	close(sn.runDoneCh)
}

func (sn *stageNode) cancelRun() {
	if sn.cancelRunCtx != nil {
		sn.cancelRunCtx()
	}
}

type stageGraph struct {
	root  *stageNode
	nodes map[Stage]*stageNode
}

func newStageGraph() *stageGraph {
	return &stageGraph{
		root:  newStageNode(nil),
		nodes: make(map[Stage]*stageNode),
	}
}

func (sg *stageGraph) build(stages []Stage) error {
	outputIndex := make(map[uintptr]*stageNode)

	for _, stage := range stages {
		node := newStageNode(stage)

		if _, ok := sg.nodes[stage]; ok {
			return fmt.Errorf("duplicated stage")
		}

		sg.nodes[stage] = node

		// Wire parent edges
		for _, in := range stage.Inputs() {
			parent, ok := outputIndex[in]
			if !ok {
				// The current stage is not connected to any previous stage
				return fmt.Errorf("stage is not connected to any previous stage")
			}

			parent.addChild(node)
		}

		// Register outputs for downstream stages to find
		for _, out := range stage.Outputs() {
			if _, ok := outputIndex[out]; ok {
				return fmt.Errorf("connector is already in use")
			}

			outputIndex[out] = node
		}
	}

	for _, node := range sg.nodes {
		if node.parentCount() == 0 {
			node.isIngress = true
			sg.root.addChild(node)
		}
	}

	return nil
}

func (sg *stageGraph) traverse() iter.Seq[*stageNode] {
	return func(yield func(*stageNode) bool) {
		inDegree := make(map[*stageNode]int)
		for _, node := range sg.nodes {
			inDegree[node] = node.parentCount()
		}
		inDegree[sg.root] = 0

		queue := []*stageNode{sg.root}
		for len(queue) > 0 {
			node := queue[0]
			queue = queue[1:]

			// Skip the root node
			stage := node.stage
			if stage != nil && !yield(node) {
				return
			}

			for _, child := range node.children {
				inDegree[child]--
				if inDegree[child] == 0 {
					queue = append(queue, child)
				}
			}
		}
	}
}
