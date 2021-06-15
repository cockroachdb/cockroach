// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

// Option is an interface used to configure operations against a Cluster.
//
// TODO(tschottdorf): Consider using a more idiomatic approach in which options
// act upon a config struct:
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type Option interface {
	Option() // TODO(tbg): lower-case this again when all opts are in this pkg
}
