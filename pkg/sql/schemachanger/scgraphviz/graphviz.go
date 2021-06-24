// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraphviz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/emicklei/dot"
	"github.com/gogo/protobuf/jsonpb"
)

// DrawStages returns a graphviz string of the stages of the Plan.
func DrawStages(p *scplan.Plan) (string, error) {
	gv, err := drawStages(p)
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

// DrawDependencies returns a graphviz string of graph used to build the Plan.
func DrawDependencies(p *scplan.Plan) (string, error) {
	gv, err := drawDeps(p)
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

func drawStages(p *scplan.Plan) (*dot.Graph, error) {

	dg := dot.NewGraph()
	stagesSubgraph := dg.Subgraph("stages", dot.ClusterOption{})
	targetsSubgraph := stagesSubgraph.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[*scpb.Target]dot.Node, len(p.Initial))
	for idx, n := range p.Initial {
		t := n.Target
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t.Element()))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = tn
	}

	// Want to draw an edge to the initial target statuses with some dots
	// or something.
	curNodes := make([]dot.Node, len(p.Initial))
	cur := p.Initial
	for i, n := range p.Initial {
		label := targetStatusID(i, n.Status)
		tsn := stagesSubgraph.Node(fmt.Sprintf("initial %d", i))
		tsn.Attr("label", label)
		tn := targetNodes[n.Target]
		e := tn.Edge(tsn)
		e.Dashed()
		e.Label(n.Target.Direction.String())
		curNodes[i] = tsn
	}
	for id, st := range p.Stages {
		stage := fmt.Sprintf("stage %d", id)
		sg := stagesSubgraph.Subgraph(stage, dot.ClusterOption{})
		next := st.After
		nextNodes := make([]dot.Node, len(curNodes))
		for i, st := range next {
			cst := sg.Node(fmt.Sprintf("stage %d: %d", id, i))
			cst.Attr("label", targetStatusID(i, st.Status))
			if st != cur[i] {
				ge := curNodes[i].Edge(cst)
				oe, ok := p.Graph.GetOpEdgeFrom(cur[i])
				if ok {
					ge.Attr("label", htmlLabel(oe.Op()))
					ge.Attr("fontsize", "9")
				}
			} else {
				ge := curNodes[i].Edge(cst)
				ge.Dotted()
			}
			nextNodes[i] = cst
		}
		cur, curNodes = next, nextNodes
	}

	return dg, nil
}

func drawDeps(p *scplan.Plan) (*dot.Graph, error) {
	dg := dot.NewGraph()

	depsSubgraph := dg.Subgraph("deps", dot.ClusterOption{})
	targetsSubgraph := depsSubgraph.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[*scpb.Target]dot.Node, len(p.Initial))
	targetIdxMap := make(map[*scpb.Target]int)
	for idx, n := range p.Initial {
		t := n.Target
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t.Element()))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = tn
		targetIdxMap[t] = idx
	}

	nodeNodes := make(map[*scpb.Node]dot.Node)
	_ = p.Graph.ForEachNode(func(n *scpb.Node) error {
		nodeNodes[n] = depsSubgraph.Node(targetStatusID(targetIdxMap[n.Target], n.Status))
		return nil
	})

	for _, n := range p.Initial {
		nn := nodeNodes[n]
		tn := targetNodes[n.Target]
		e := tn.Edge(nn)
		e.Label(n.Target.Direction.String())
		e.Dashed()
	}

	_ = p.Graph.ForEachEdge(func(e scgraph.Edge) error {
		from := nodeNodes[e.From()]
		to := nodeNodes[e.To()]
		ge := from.Edge(to)
		switch e := e.(type) {
		case *scgraph.OpEdge:
			ge.Attr("label", htmlLabel(e.Op()))
			ge.Attr("fontsize", "9")
		case *scgraph.DepEdge:
			ge.Attr("color", "red")
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

// ToMap converts a struct to a map, field by field. If at any point a protobuf
// message is encountered, it is converted to a map using jsonpb to marshal it
// to json and then marshaling it back to a map. This approach allows zero
// values to be effectively omitted.
func ToMap(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	if msg, ok := v.(protoutil.Message); ok {
		var buf bytes.Buffer
		jsonEncoder := jsonpb.Marshaler{EmitDefaults: false}
		if err := jsonEncoder.Marshal(&buf, msg); err != nil {
			return nil, errors.Wrapf(err, "%T %v", v, v)
		}
		var m map[string]interface{}
		if err := json.NewDecoder(&buf).Decode(&m); err != nil {
			return nil, err
		}
		return m, nil
	}
	vv := reflect.ValueOf(v)
	vt := vv.Type()
	switch vt.Kind() {
	case reflect.Struct:
	case reflect.Ptr:
		if vt.Elem().Kind() != reflect.Struct {
			return v, nil
		}
		vv = vv.Elem()
		vt = vt.Elem()
	default:
		return v, nil
	}

	m := make(map[string]interface{}, vt.NumField())
	for i := 0; i < vt.NumField(); i++ {
		vvf := vv.Field(i)
		if !vvf.CanInterface() || vvf.IsZero() {
			continue
		}
		var err error
		if m[vt.Field(i).Name], err = ToMap(vvf.Interface()); err != nil {
			return nil, err
		}
	}
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
