// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scgraphviz

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"html/template"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scstage"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
	"github.com/emicklei/dot"
)

// StagesURL returns a URL to a rendering of the stages of the Plan.
func StagesURL(cs scpb.CurrentState, g *scgraph.Graph, stages []scstage.Stage) (string, error) {
	gv, err := DrawStages(cs, g, stages)
	if err != nil {
		return "", err
	}
	return buildURL(gv)
}

// DependenciesURL returns a URL to a rendering of the graph used to build
// the Plan.
func DependenciesURL(cs scpb.CurrentState, g *scgraph.Graph) (string, error) {
	gv, err := DrawDependencies(cs, g)
	if err != nil {
		return "", err
	}
	return buildURL(gv)
}

func buildURL(gv string) (string, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := io.WriteString(w, gv); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return (&url.URL{
		Scheme:   "https",
		Host:     "cockroachdb.github.io",
		Path:     "scplan/viz.html",
		Fragment: base64.StdEncoding.EncodeToString(buf.Bytes()),
	}).String(), nil
}

// DrawStages returns a graphviz string of the stages of the Plan.
func DrawStages(cs scpb.CurrentState, g *scgraph.Graph, stages []scstage.Stage) (string, error) {
	if len(stages) == 0 {
		return "", errors.Errorf("missing stages in plan")
	}
	gv, err := drawStages(cs, g, stages)
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

// DrawDependencies returns a graphviz string of graph used to build the Plan.
func DrawDependencies(cs scpb.CurrentState, g *scgraph.Graph) (string, error) {
	if g == nil {
		return "", errors.Errorf("missing graph in plan")
	}
	gv, err := drawDeps(cs, g)
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

func drawStages(
	cs scpb.CurrentState, g *scgraph.Graph, stages []scstage.Stage,
) (*dot.Graph, error) {
	dg := dot.NewGraph()
	stagesSubgraph := dg.Subgraph("stages", dot.ClusterOption{})
	targetsSubgraph := stagesSubgraph.Subgraph("targets", dot.ClusterOption{})
	statementsSubgraph := stagesSubgraph.Subgraph("statements", dot.ClusterOption{})
	targetNodes := make([]dot.Node, len(cs.TargetState.Targets))
	// Add all the statements in their own section.
	// Note: Explains can only have one statement, so we aren't
	// going to bother adding arrows to them.
	for idx, stmt := range cs.TargetState.Statements {
		stmtNode := statementsSubgraph.Node(itoa(idx, len(cs.TargetState.Statements)))
		stmtNode.Attr("label", htmlLabel(stmt))
		stmtNode.Attr("fontsize", "9")
		stmtNode.Attr("shape", "none")
	}
	for idx, t := range cs.TargetState.Targets {
		tn := targetsSubgraph.Node(itoa(idx, len(cs.TargetState.Targets)))
		tn.Attr("label", htmlLabel(t.Element()))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[idx] = tn
	}

	// Want to draw an edge to the initial target statuses
	// with some dots or something.
	curNodes := make([]dot.Node, len(cs.Initial))
	cur := cs.Initial
	curDummy := targetsSubgraph.Node("dummy")
	curDummy.Attr("shape", "point")
	curDummy.Attr("style", "invis")
	for i, status := range cs.Initial {
		label := targetStatusID(i, status)
		tsn := stagesSubgraph.Node(fmt.Sprintf("initial %d", i))
		tsn.Attr("label", label)
		tn := targetNodes[i]
		e := tn.Edge(tsn)
		e.Dashed()
		e.Label(fmt.Sprintf("to %s", cs.TargetState.Targets[i].TargetStatus.String()))
		curNodes[i] = tsn
	}
	for _, st := range stages {
		stage := st.String()
		sg := stagesSubgraph.Subgraph(stage, dot.ClusterOption{})
		next := st.After
		nextNodes := make([]dot.Node, len(curNodes))
		m := make(map[scpb.Element][]scop.Op, len(curNodes))
		for _, op := range st.EdgeOps {
			if oe := g.GetOpEdgeFromOp(op); oe != nil {
				e := oe.To().Element()
				m[e] = append(m[e], op)
			}
		}

		for i, status := range next {
			cst := sg.Node(fmt.Sprintf("%s: %d", stage, i))
			cst.Attr("label", targetStatusID(i, status))
			ge := curNodes[i].Edge(cst)
			if status != cur[i] {
				if ops := m[cs.TargetState.Targets[i].Element()]; len(ops) > 0 {
					ge.Attr("label", htmlLabel(ops))
					ge.Attr("fontsize", "9")
				}
			} else {
				ge.Dotted()
			}
			nextNodes[i] = cst
		}
		nextDummy := sg.Node(fmt.Sprintf("%s: dummy", stage))
		nextDummy.Attr("shape", "point")
		nextDummy.Attr("style", "invis")
		if len(st.ExtraOps) > 0 {
			ge := curDummy.Edge(nextDummy)
			ge.Attr("label", htmlLabel(st.ExtraOps))
			ge.Attr("fontsize", "9")
		}
		cur, curNodes, curDummy = next, nextNodes, nextDummy
	}

	return dg, nil
}

func drawDeps(cs scpb.CurrentState, g *scgraph.Graph) (*dot.Graph, error) {
	dg := dot.NewGraph()

	depsSubgraph := dg.Subgraph("deps", dot.ClusterOption{})
	targetsSubgraph := depsSubgraph.Subgraph("targets", dot.ClusterOption{})
	statementsSubgraph := depsSubgraph.Subgraph("statements", dot.ClusterOption{})
	targetNodes := make([]dot.Node, len(cs.Initial))
	targetIdxMap := make(map[*scpb.Target]int)
	// Add all the statements in their own section.
	// Note: Explains can only have one statement, so we aren't
	// going to bother adding arrows to them.
	for idx, stmt := range cs.TargetState.Statements {
		stmtNode := statementsSubgraph.Node(itoa(idx, len(cs.TargetState.Statements)))
		stmtNode.Attr("label", htmlLabel(stmt))
		stmtNode.Attr("fontsize", "9")
		stmtNode.Attr("shape", "none")
	}
	targetStatusNodes := make([]map[scpb.Status]dot.Node, len(cs.Initial))
	for idx, status := range cs.Initial {
		t := &cs.TargetState.Targets[idx]
		tn := targetsSubgraph.Node(itoa(idx, len(cs.Initial)))
		tn.Attr("label", htmlLabel(t.Element()))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[idx] = tn
		targetIdxMap[t] = idx
		targetStatusNodes[idx] = map[scpb.Status]dot.Node{status: tn}
	}
	_ = g.ForEachNode(func(n *screl.Node) error {
		tn := depsSubgraph.Node(targetStatusID(targetIdxMap[n.Target], n.CurrentStatus))
		targetStatusNodes[targetIdxMap[n.Target]][n.CurrentStatus] = tn
		return nil
	})
	for idx, status := range cs.Initial {
		nn := targetStatusNodes[idx][status]
		tn := targetNodes[idx]
		e := tn.Edge(nn)
		e.Label(fmt.Sprintf("to %s", cs.TargetState.Targets[idx].TargetStatus.String()))
		e.Dashed()
	}

	_ = g.ForEachEdge(func(e scgraph.Edge) error {
		from := targetStatusNodes[targetIdxMap[e.From().Target]][e.From().CurrentStatus]
		to := targetStatusNodes[targetIdxMap[e.To().Target]][e.To().CurrentStatus]
		ge := from.Edge(to)
		switch e := e.(type) {
		case *scgraph.OpEdge:
			ge.Attr("label", htmlLabel(e.Op()))
			ge.Attr("fontsize", "9")
		case *scgraph.DepEdge:
			ge.Attr("color", "red")
			ge.Attr("label", e.RuleNames())
			switch e.Kind() {
			case scgraph.PreviousStagePrecedence, scgraph.PreviousTransactionPrecedence:
				ge.Attr("arrowhead", "inv")
			case scgraph.SameStagePrecedence:
				ge.Attr("arrowhead", "diamond")
			}
		}
		return nil
	})
	return dg, nil
}

func targetStatusID(targetID int, status scpb.Status) string {
	return fmt.Sprintf("%d:%s", targetID, status)
}

func htmlLabel(o interface{}) dot.HTML {
	var buf strings.Builder
	if err := objectTemplate.Execute(&buf, o); err != nil {
		panic(err)
	}
	return dot.HTML(buf.String())
}

func itoa(i, ub int) string {
	return fmt.Sprintf(fmt.Sprintf("%%0%dd", len(strconv.Itoa(ub))), i)
}

// ToMap is a thin wrapper around scviz.ToMap which comes with some default
// behavior for planned data structures.
func ToMap(v interface{}) (interface{}, error) {
	// The SetJobStateOnDescriptor is very large and graphviz fails to render it.
	// Clear the DescriptorState field so that the relevant information (the
	// existence of the Op and the descriptor ID) make it into the graph.
	if sjs, ok := v.(*scop.SetJobStateOnDescriptor); ok {
		clone := *sjs
		clone.State = scpb.DescriptorState{}
		v = &clone
	}
	m, err := scviz.ToMap(v, false /* emitDefaults */)
	if err != nil {
		return nil, err
	}
	scviz.WalkMap(m, scviz.RewriteEmbeddedIntoParent)
	return m, nil
}

var objectTemplate = template.Must(template.New("obj").Funcs(template.FuncMap{
	"typeOf": func(v interface{}) string {
		return fmt.Sprintf("%T", v)
	},
	"isMap": func(v interface{}) bool {
		_, ok := v.(map[string]interface{})
		return ok
	},
	"isSlice": func(v interface{}) bool {
		vv := reflect.ValueOf(v)
		if !vv.IsValid() {
			return false
		}
		return vv.Kind() == reflect.Slice
	},
	"emptyMap": func(v interface{}) bool {
		m, ok := v.(map[string]interface{})
		return ok && len(m) == 0
	},
	"emptySlice": func(v interface{}) bool {
		m, ok := v.([]interface{})
		return ok && len(m) == 0
	},
	"isStruct": func(v interface{}) bool {
		return reflect.Indirect(reflect.ValueOf(v)).Kind() == reflect.Struct
	},
	"toMap": ToMap,
}).Parse(`
{{- define "key" -}}
<td>
{{- . -}}
</td>
{{- end -}}

{{- define "val" -}}
<td>
{{- if (isMap .) -}}
{{- template "mapVal" . -}}
{{- else if (isSlice .) -}}
{{- template "sliceVal" . -}}
{{- else if (isStruct .) -}}
<td>
{{- typeOf . -}}
</td>
<td>
{{- template "mapVal" (toMap .) -}}
</td>
{{- else -}}
{{- . -}}
{{- end -}}
</td>
{{- end -}}

{{- define "sliceVal" -}}
{{- if not (emptySlice .) -}}
<table class="val"><tr>
{{- range . -}}
{{- template "val" . -}}
{{- end -}}
</tr></table>
{{- end -}}
{{- end -}}

{{- define "mapVal" -}}
<table class="table">
{{- range $k, $v :=  . -}}
{{- if not (emptyMap $v) -}}
<tr>
{{- template "key" $k -}}
{{- if (isStruct $v) -}}
<td>
{{- typeOf . -}}
</td>
<td>
{{- template "mapVal" (toMap $v) -}}
</td>
{{- else -}}
{{- template "val" $v -}}
{{- end -}}
</tr>
{{- end -}}
{{- end -}}
</table>
{{- end -}}

{{- define "header" -}}
<tr><td class="header">
{{- typeOf . -}}
</td></tr>
{{- end -}}

<table class="outer">
{{- template "header" . -}}
<tr><td>
{{- template "mapVal" (toMap .) -}}
</td></tr>
</table>
`))
