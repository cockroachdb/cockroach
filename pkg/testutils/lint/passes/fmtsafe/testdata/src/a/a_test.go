// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package a

import (
	"testing"

	"github.com/cockroachdb/errors"
)

func Test(t *testing.T) {
	_ = errors.New(unsafeStr) // want `message argument is not a constant expression`
	_ = errors.New(unsafeStr /*nolint:fmtsafe*/)
}
