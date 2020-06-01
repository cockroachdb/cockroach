// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

// UseSphereOrSpheroid indicates whether to use a Sphere or Spheroid
// for certain calculations.
type UseSphereOrSpheroid bool

const (
	// UseSpheroid indicates to use the spheroid for calculations.
	UseSpheroid UseSphereOrSpheroid = true
	// UseSphere indicates to use the sphere for calculations.
	UseSphere UseSphereOrSpheroid = false
)
