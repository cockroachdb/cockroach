package dot

// Node represents a dot Node.
type Node struct {
	AttributesMap
	graph *Graph
	id    string
	seq   int
}

// Attr sets label=value and return the Node
func (n Node) Attr(label string, value interface{}) Node {
	n.AttributesMap.Attr(label, value)
	return n
}

// Label sets the attribute "label" to the given label
func (n Node) Label(label string) Node {
	return n.Attr("label", label)
}

// Box sets the attribute "shape" to "box"
func (n Node) Box() Node {
	return n.Attr("shape", "box")
}

// Edge sets label=value and returns the Edge for chaining.
func (n Node) Edge(toNode Node, labels ...string) Edge {
	return n.graph.Edge(n, toNode, labels...)
}

// EdgesTo returns all existing edges between this Node and the argument Node.
func (n Node) EdgesTo(toNode Node) []Edge {
	return n.graph.FindEdges(n, toNode)
}
