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

func (g *targetStateGraph) draw() (*dot.Graph, error) {
	dg := dot.NewGraph()
	targetsSubgraph := dg.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[targets.Target]*dot.Node, len(g.targets))
	for idx, t := range g.targets {
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = &tn
	}
	panic("unimplemented")
	//for _, ts := range g.initialTargetStates {
	//
	//}
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
		return m, nil
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
