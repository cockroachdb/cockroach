// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"gonum.org/v1/gonum/floats/scalar"
)

// ValidateUnitVector panics if the given vector is not a unit vector or the
// zero vector (for degenerate case where norm=0).
// NOTE: This should only be used in test builds.
func ValidateUnitVector(vec vector.T) {
	if buildutil.CrdbTestBuild {
		norm := num32.SquaredNorm(vec)
		if norm != 0 && scalar.Round(float64(norm), 2) != 1 {
			panic(errors.AssertionFailedf("vector is not a unit vector: %s", vec))
		}
	}
}

// ValidateUnitVectors panics if the given vectors are not unit vectors or zero
// vectors (for degenerate case where norm=0).
// NOTE: This should only be used in test builds.
func ValidateUnitVectors(vectors vector.Set) {
	if buildutil.CrdbTestBuild {
		for i := range vectors.Count {
			ValidateUnitVector(vectors.At(i))
		}
	}
}
