package goccia

import (
	"context"
	"testing"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/stretchr/testify/assert"
)

var nextConn uintptr = 1

func newConn() uintptr {
	id := nextConn
	nextConn++
	return id
}

var _ Stage = (*mockStage)(nil)

type mockStage struct {
	name    string
	inputs  []uintptr
	outputs []uintptr
}

func newMockStage(name string, inputs, outputs []uintptr) *mockStage {
	return &mockStage{
		name:    name,
		inputs:  inputs,
		outputs: outputs,
	}
}

func (m *mockStage) Kind() stage.Kind                { return stage.KindIngress }
func (m *mockStage) Name() string                    { return m.name }
func (m *mockStage) Telemetry() *telemetry.Telemetry { return nil }
func (m *mockStage) Config() config.Config           { return nil }
func (m *mockStage) Init(_ context.Context) error    { return nil }
func (m *mockStage) Run(_ context.Context)           {}
func (m *mockStage) Close()                          {}
func (m *mockStage) Inputs() []uintptr               { return m.inputs }
func (m *mockStage) Outputs() []uintptr              { return m.outputs }

func collectNames(sg *stageGraph) []string {
	var names []string
	for stageNode := range sg.traverse() {
		names = append(names, stageNode.stage.(*mockStage).name)
	}
	return names
}

// ─── Linear ─────────────────────────────────────────────────────────────────|

// A → B → C
// Expected: A, B, C
func Test_stageGraph_Linear(t *testing.T) {
	assert := assert.New(t)

	ab, bc := newConn(), newConn()

	a := newMockStage("A", nil, []uintptr{ab})
	b := newMockStage("B", []uintptr{ab}, []uintptr{bc})
	c := newMockStage("C", []uintptr{bc}, nil)

	sg := newStageGraph()
	assert.NoError(sg.build([]Stage{a, b, c}))

	result := collectNames(sg)
	expected := []string{"A", "B", "C"}

	assert.Equal(expected, result)
}

// ─── Fan-Out ────────────────────────────────────────────────────────────────|

// A → B
// A → C
// Expected: A comes before both B and C. B and C order is non-deterministic.
func Test_stageGraph_FanOut(t *testing.T) {
	assert := assert.New(t)

	ab, ac := newConn(), newConn()

	a := newMockStage("A", nil, []uintptr{ab, ac})
	b := newMockStage("B", []uintptr{ab}, nil)
	c := newMockStage("C", []uintptr{ac}, nil)

	sg := newStageGraph()
	assert.NoError(sg.build([]Stage{a, b, c}))

	result := collectNames(sg)

	assert.Len(result, 3)
	assert.Equal("A", result[0])
	assert.Contains(result[1:], "B")
	assert.Contains(result[1:], "C")
}

// ─── Fan-In ─────────────────────────────────────────────────────────────────|

// A → C
// B → C
// Expected: A and B before C. A and B order is non-deterministic.
func Test_stageGraph_FanIn(t *testing.T) {
	assert := assert.New(t)

	ac, bc := newConn(), newConn()

	a := newMockStage("A", nil, []uintptr{ac})
	b := newMockStage("B", nil, []uintptr{bc})
	c := newMockStage("C", []uintptr{ac, bc}, nil)

	sg := newStageGraph()
	assert.NoError(sg.build([]Stage{a, b, c}))

	result := collectNames(sg)

	assert.Len(result, 3)
	assert.Contains(result[:2], "A")
	assert.Contains(result[:2], "B")
	assert.Equal("C", result[2])
}

// ─── Diamond ────────────────────────────────────────────────────────────────|

// A → B
// A → C
// B → D
// C → D
// Expected: A first, D last, B and C in the middle.
func Test_stageGraph_Diamond(t *testing.T) {
	assert := assert.New(t)

	ab, ac, bd, cd := newConn(), newConn(), newConn(), newConn()

	a := newMockStage("A", nil, []uintptr{ab, ac})
	b := newMockStage("B", []uintptr{ab}, []uintptr{bd})
	c := newMockStage("C", []uintptr{ac}, []uintptr{cd})
	d := newMockStage("D", []uintptr{bd, cd}, nil)

	sg := newStageGraph()
	assert.NoError(sg.build([]Stage{a, b, c, d}))

	result := collectNames(sg)

	assert.Len(result, 4)
	assert.Equal("A", result[0])
	assert.Contains(result[1:3], "B")
	assert.Contains(result[1:3], "C")
	assert.Equal("D", result[3])
}

// ─── Multiple Roots ─────────────────────────────────────────────────────────|

// A → C
// B → C
// C → D
// Expected: A and B before C, C before D.
func Test_stageGraph_MultipleRoots(t *testing.T) {
	assert := assert.New(t)

	ac, bc, cd := newConn(), newConn(), newConn()

	a := newMockStage("A", nil, []uintptr{ac})
	b := newMockStage("B", nil, []uintptr{bc})
	c := newMockStage("C", []uintptr{ac, bc}, []uintptr{cd})
	d := newMockStage("D", []uintptr{cd}, nil)

	sg := newStageGraph()
	assert.NoError(sg.build([]Stage{a, b, c, d}))

	result := collectNames(sg)

	assert.Len(result, 4)
	assert.Contains(result[:2], "A")
	assert.Contains(result[:2], "B")
	assert.Equal("C", result[2])
	assert.Equal("D", result[3])
}

// ─── Build Errors ───────────────────────────────────────────────────────────|

func Test_stageGraph_BuildErrors(t *testing.T) {
	assert := assert.New(t)

	// Disconnected stage
	sg := newStageGraph()

	a := newMockStage("A", nil, []uintptr{newConn()})
	b := newMockStage("B", []uintptr{newConn()}, nil)

	assert.Error(sg.build([]Stage{a, b}))

	// Duplicated stage
	sg = newStageGraph()
	a = newMockStage("A", nil, []uintptr{newConn()})
	assert.Error(sg.build([]Stage{a, a}))

	// Output connector already used
	sg = newStageGraph()
	shared := newConn()

	a = newMockStage("A", nil, []uintptr{shared})
	b = newMockStage("B", nil, []uintptr{shared})

	assert.Error(sg.build([]Stage{a, b}))

	// Input connector already used
	sg = newStageGraph()
	shared = newConn()

	a = newMockStage("A", []uintptr{shared}, nil)
	b = newMockStage("B", []uintptr{shared}, nil)

	assert.Error(sg.build([]Stage{a, b}))
}
