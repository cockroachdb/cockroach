package dot

// Edge represents a graph edge between two Nodes.
type Edge struct {
	AttributesMap
	graph    *Graph
	from, to Node
}

// Attr sets key=value and returns the Egde.
func (e Edge) Attr(key string, value interface{}) Edge {
	e.AttributesMap.Attr(key, value)
	return e
}

// Label sets "label"=value and returns the Edge.
// Same as Attr("label",value)
func (e Edge) Label(value interface{}) Edge {
	e.AttributesMap.Attr("label", value)
	return e
}

// Solid sets the edge attribute "style" to "solid"
// Default style
func (e Edge) Solid() Edge {
	return e.Attr("style", "solid")
}

// Bold sets the edge attribute "style" to "bold"
func (e Edge) Bold() Edge {
	return e.Attr("style", "bold")
}

// Dashed sets the edge attribute "style" to "dashed"
func (e Edge) Dashed() Edge {
	return e.Attr("style", "dashed")
}

// Dotted sets the edge attribute "style" to "dotted"
func (e Edge) Dotted() Edge {
	return e.Attr("style", "dotted")
}

// Edge returns a new Edge between the "to" node of this Edge and the argument Node.
func (e Edge) Edge(to Node, labels ...string) Edge {
	return e.graph.Edge(e.to, to, labels...)
}

// EdgesTo returns all existing edges between the "to" Node of thie Edge and the argument Node.
func (e Edge) EdgesTo(to Node) []Edge {
	return e.graph.FindEdges(e.to, to)
}
