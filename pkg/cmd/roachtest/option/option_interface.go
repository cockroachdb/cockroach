// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package option

// Option is an interface used to configure operations against a Cluster.
//
// TODO(tschottdorf): Consider using a more idiomatic approach in which options
// act upon a config struct:
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type Option interface {
	Option() // TODO(tbg): lower-case this again when all opts are in this pkg
}
