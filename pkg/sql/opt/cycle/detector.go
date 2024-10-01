// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cycle

// Detector finds cycles in directed graphs. Vertices are identified as unique
// integers by the caller.
//
// Example usage:
//
//	var d Detector
//	d.AddEdge(Vertex(0), Vertex(1))
//	d.AddEdge(Vertex(1), Vertex(2))
//	d.AddEdge(Vertex(2), Vertex(0))
//	d.FindCycleStartingAtVertex(Vertex(0))
//	=> [0, 1, 2, 0], true
type Detector struct {
	// edges are the directed edges in the graph.
	edges map[Vertex][]Vertex
}

// Vertex is a vertex in the graph.
type Vertex int

// status represents one of three possible states of a vertex during cycle
// detection.
type status int

const (
	// statusNotVisited indicates that a vertex has not been previously visited.
	statusNotVisited status = iota
	// statusVisited indicates that a vertex has been visited but a verdict on
	// its involvement in a cycle has not yet been reached.
	statusVisited
	// statusDone indicates that a vertex has been visited and has been proven
	// innocent of being involved in a cycle.
	statusDone
)

// AddEdge adds a directed edge to the graph.
func (cd *Detector) AddEdge(from, to Vertex) {
	if cd.edges == nil {
		cd.edges = make(map[Vertex][]Vertex)
	}
	cd.edges[from] = append(cd.edges[from], to)
}

// FindCycleStartingAtVertex searches for a cycle starting from v. If one is
// found, a path containing that cycle is returned. After finding the first
// cycle, searching ceases. If no cycle is found, ok=false is returned.
func (cd *Detector) FindCycleStartingAtVertex(v Vertex) (cycle []Vertex, ok bool) {
	vertices := make(map[Vertex]status)
	var s stack
	if ok := cd.hasCycle(v, vertices, &s); ok {
		// If a cycle was found, s will contain a path that includes the cycle.
		return s, true
	}
	return nil, false
}

// hasCycle performs a depth-first search of the graph starting at v in search
// of a cycle. It returns true when the first cycle is found. If a cycle was
// found, s contains a path that includes the cycle. Cycles are detected by
// finding edges that backtrack to a previously visited vertex.
func (cd *Detector) hasCycle(v Vertex, vertices map[Vertex]status, s *stack) bool {
	// Mark the given Vertex as visited and push it onto the stack.
	vertices[v] = statusVisited
	s.push(v)

	// Search all children of v for cycles.
	for _, child := range cd.edges[v] {
		switch vertices[child] {
		case statusVisited:
			// A cycle has been found. The current stack is a path to the cycle.
			// Push the child onto the stack so that the cycle is more obvious
			// in the path.
			s.push(child)
			return true
		case statusNotVisited:
			// Recurse if this child of v has not yet been visited.
			if ok := cd.hasCycle(child, vertices, s); ok {
				// If a cycle was found deeper down, propagate the result and
				// halt searching other children of v.
				return true
			}
		case statusDone:
			// We have already proven that this child does not backtrack to
			// anywhere further up the stack, so we do not need to recurse into
			// it.
		}
	}

	// If we didn't return early from the loop, we did not find a cycle below
	// this vertex, so we can mark the vertex as done and pop it off the stack.
	vertices[v] = statusDone
	s.pop()
	return false
}

// stack is a simple implementation of a stack of vertices. It allows more
// ergonomic mutation by callees than a slice.
type stack []Vertex

func (s *stack) push(i Vertex) {
	*s = append(*s, i)
}

func (s *stack) pop() {
	*s = (*s)[:len(*s)-1]
}
