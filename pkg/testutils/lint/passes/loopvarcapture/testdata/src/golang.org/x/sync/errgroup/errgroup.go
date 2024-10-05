// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errgroup

type Group struct{}

func (g *Group) Go(f func() error) {
	go f()
}
