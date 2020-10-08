// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

var (
	f  float64 = -1
	ff         = float64(f) // want `unnecessary conversion`
	fi         = int(f)

	// nolint:unconvert
	fff = float64(f)

	// nolint:unconvert
	_ = float64(f) +
		float64(f)

	_ = 1 +
		// nolint:unconvert
		float64(f) +
		2

	_ = 1 +
		2 +
		float64(f) // nolint:unconvert

	_ = 1 +
		float64(f) + // nolint:unconvert
		2

	_ = 1 +
		float64(f) + 2 // nolint:unconvert

	_ = 1 +
		float64(f) + 2 + 3 // nolint:unconvert

	_ = 1 +
		(float64(f) + 2 +
			3) // nolint:unconvert

	_ = 1 +
		2 +
		3 +
		// nolint:unconvert
		float64(f)

	// nolint:unconvert
	_ = 1 +
		2 +
		3 +
		float64(f)

	_ = 1 +
		(float64(f) /* nolint:unconvert */ + 2 +
			3)

	// Here the comment does not cover the conversion.

	_ = 1 +
		// nolint:unconvert
		2 +
		3 +
		float64(f) // want `unnecessary conversion`

	_ = 1 +
		// nolint:unconvert
		2 +
		float64(f) // want `unnecessary conversion`

	_ = 1 +
		(float64(f) + 2 + // want `unnecessary conversion`
			3 /* nolint:unconvert */)

	_ = 1 +
		(float64(f) + 2 /* nolint:unconvert */ + // want `unnecessary conversion`
			3)

	_ = 1 +
		(float64(f) + 2 + /* nolint:unconvert */ // want `unnecessary conversion`
			3)

	_ = 1 +
		(float64(f) + 2 + // nolint:unconvert // want `unnecessary conversion`
			3)

	_ = 1 +
		2 + // nolint:unconvert
		float64(f) // want `unnecessary conversion`
)

func foo() {
	// nolint:unconvert
	if fff := float64(f); fff > 0 {
		panic("foo")
	}

	if fff := float64(f); fff > 0 { // want `unnecessary conversion`
		panic("foo")
	}
}
