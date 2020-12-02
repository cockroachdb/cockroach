package compiler

import (
	"encoding/json"
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/emicklei/dot"
)

func (g *targetStateGraph) drawStages() (*dot.Graph, error) {

	dg := dot.NewGraph()
	stagesSubgraph := dg.Subgraph("stages", dot.ClusterOption{})
	targetsSubgraph := stagesSubgraph.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[targets.Target]dot.Node, len(g.targets))
	for idx, t := range g.targets {
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = tn
	}

	// Want to draw an edge to the initial target states with some dots
	// or something.
	curNodes := make([]dot.Node, len(g.initialTargetStates))
	cur := g.initialTargetStates
	for i, ts := range g.initialTargetStates {
		label := targetStateID(g.targetIdxMap[ts.Target], ts.State)
		tsn := stagesSubgraph.Node(fmt.Sprintf("initial %d", i))
		tsn.Attr("label", label)
		tn := targetNodes[ts.Target]
		e := tn.Edge(tsn)
		e.Dashed()
		curNodes[i] = tsn
	}
	for id, st := range g.stages {
		stage := fmt.Sprintf("stage %d", id)
		sg := stagesSubgraph.Subgraph(stage, dot.ClusterOption{})
		next := st.next
		nextNodes := make([]dot.Node, len(curNodes))
		for i, st := range next {
			cst := sg.Node(fmt.Sprintf("stage %d: %d", id, i))
			cst.Attr("label", targetStateID(i, st.State))
			if st != cur[i] {
				ge := curNodes[i].Edge(cst)
				ge.Attr("label", htmlLabel(g.targetStateOpEdges[cur[i]].op))
				ge.Attr("fontsize", "9")
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

func (g *targetStateGraph) drawDeps() (*dot.Graph, error) {
	dg := dot.NewGraph()

	depsSubgraph := dg.Subgraph("deps", dot.ClusterOption{})
	targetsSubgraph := depsSubgraph.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[targets.Target]dot.Node, len(g.targets))
	for idx, t := range g.targets {
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = tn
	}

	tsNodes := make(map[*targets.TargetState]dot.Node)
	for _, tsMap := range g.targetStates {
		for _, ts := range tsMap {
			tsNodes[ts] = depsSubgraph.Node(targetStateID(g.targetIdxMap[ts.Target], ts.State))
		}
	}

	// Want to draw an edge to the initial target states with some dots
	// or something.

	for _, ts := range g.initialTargetStates {
		tsn := tsNodes[ts]
		tn := targetNodes[ts.Target]
		e := tn.Edge(tsn)
		e.Dashed()
	}

	for _, e := range g.edges {
		from := tsNodes[e.start()]
		to := tsNodes[e.end()]
		ge := from.Edge(to)
		switch e := e.(type) {
		case *opEdge:
			ge.Attr("label", htmlLabel(e.op))
			ge.Attr("fontsize", "9")
		case *depEdge:
			ge.Attr("color", "red")
		}
	}
	return dg, nil

}

func drawGraph(edges []edge) (string, error) {
	g := dot.NewGraph()
	for _, e := range edges {
		from, to := e.start(), e.end()
		fromID, toID := jsonLabel(from.Target), jsonLabel(to.Target)

		fromG := g.Subgraph(string(fromID), dot.ClusterOption{})
		fromG.Attr("label", htmlLabel(from.Target))
		fromG.Attr("shape", "none")
		fromG.Attr("fontsize", "9")

		toG := g.Subgraph(string(toID), dot.ClusterOption{})
		toG.Attr("label", htmlLabel(to.Target))
		toG.Attr("shape", "none")
		toG.Attr("fontsize", "9")

		fromN := fromG.Node(from.State.String())
		toN := toG.Node(to.State.String())
		var ge dot.Edge
		if fromG == toG {
			ge = fromG.Edge(fromN, toN)
		} else {
			ge = g.Edge(fromN, toN)
		}
		switch e := e.(type) {
		case *opEdge:
			ge.Attr("label", htmlLabel(e.op))
			ge.Attr("fontsize", "9")
		case *depEdge:
			ge.Attr("color", "red")
		}
	}
	var buf strings.Builder
	g.Write(&buf)
	return buf.String(), nil
}

func targetStateID(targetID int, state targets.State) string {
	return fmt.Sprintf("%d:%s", targetID, state)
}

func jsonLabel(o interface{}) dot.Literal {
	raw, err := json.MarshalIndent(o, "", "    ")
	if err != nil {
		panic(err)
	}
	s := strings.Replace(
		fmt.Sprintf("%q", fmt.Sprintf("%T%s", o, raw)),
		`\n`, `\l`, -1)
	return dot.Literal(s[:len(s)-1] + `\l"`)
}

func htmlLabel(o interface{}) dot.HTML {
	var buf strings.Builder
	if err := objectTemplate.Execute(&buf, o); err != nil {
		panic(err)
	}
	return dot.HTML(buf.String())
}

var objectTemplate = template.Must(template.New("obj").Funcs(template.FuncMap{
	"typeOf": func(v interface{}) string {
		return fmt.Sprintf("%T", v)
	},
	"toMap": func(v interface{}) (interface{}, error) {
		marshaled, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var m map[string]interface{}
		if err := json.Unmarshal(marshaled, &m); err != nil {
			return nil, err
		}
		// Flatten objects up to one level to deal with embedded index/column
		// descriptors for now.
		flattenedMap := make(map[string]interface{})
		for k, v := range m {
			if obj, ok := v.(map[string]interface{}); ok {
				for objk, objv := range obj {
					flattenedMap[k+"."+objk] = objv
				}
			} else {
				flattenedMap[k] = v
			}
		}
		return flattenedMap, nil
	},
}).Parse(`
<table CELLSPACING="0" CELLBORDER="1" CELLPADDING="0" WIDTH="0">
  <tr>
    <td ALIGN="CENTER" COLSPAN="2" CELLSPACING="0" CELLPADDING="0">
    {{- typeOf . -}}
    </td>
  </tr>
{{ range $k, $v := (toMap .) -}}
  <tr>
    <td ALIGN="LEFT"> {{- $k -}} </td>
    <td> {{- $v -}} </td>
  </tr>
{{- end -}}
</table>
`))
