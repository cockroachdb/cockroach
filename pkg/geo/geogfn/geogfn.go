// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
