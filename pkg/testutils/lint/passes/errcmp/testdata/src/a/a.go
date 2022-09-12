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

import "github.com/cockroachdb/errors"

type myError struct{}

func (m *myError) Error() string { return "" }

var myOtherError = errors.New("a")

func f() {
	var v error
	if v == myOtherError { // want `use errors\.Is instead of a direct comparison`
		panic("here")
	}
	// nolint:errcmp
	if v == myOtherError {
		panic("here")
	}

	switch v { // want `invalid direct comparison of error object`
	case myOtherError:
		panic("here")
	}
	// nolint:errcmp
	switch v {
	case myOtherError:
		panic("here")
	}
	switch v {
	// nolint:errcmp
	case myOtherError:
		panic("here")
	}

	switch v.(type) { // want `invalid direct cast on error object`
	case *myError:
		panic("there")
	}
	// nolint:errcmp
	switch v.(type) {
	case *myError:
		panic("there")
	}
}
