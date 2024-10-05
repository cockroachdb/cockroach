// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package encodingtype

// T is declared in the encodingtype package so that it can be
// registered as always safe to report, while avoiding an import
// cycle.
type T int
