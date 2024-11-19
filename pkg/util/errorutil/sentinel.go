// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

// TempSentinel is a sentinel type that allows other packages to retrieve the
// path to this package with reflect and PkgPath.
type TempSentinel struct{}
