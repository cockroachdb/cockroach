// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func IterateGeoBuiltinOverloads(f func(builtinName string, ol []tree.Overload)) {
	for k, builtin := range geoBuiltins {
		f(k, builtin.overloads)
	}
}
