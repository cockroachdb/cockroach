// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

// FuncSourceLister is a SourceLister backed by two functions. This avoids
// tight coupling between the promql package and the server's liveness or
// store subsystems.
type FuncSourceLister struct {
	NodeSourcesFn  func() []string
	StoreSourcesFn func() []string
}

var _ SourceLister = (*FuncSourceLister)(nil)

func (f *FuncSourceLister) NodeSources() []string  { return f.NodeSourcesFn() }
func (f *FuncSourceLister) StoreSources() []string { return f.StoreSourcesFn() }
