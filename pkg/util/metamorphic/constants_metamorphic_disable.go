// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build metamorphic_disable

package metamorphic

// DisableMetamorphicTesting can be used to disable metamorphic tests. If it
// is set to true then metamorphic testing will not be enabled.
var disableMetamorphicTesting = true
