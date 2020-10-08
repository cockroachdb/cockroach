// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import (
	"testing"

	"github.com/cockroachdb/errors"
)

func Test(t *testing.T) {
	_ = errors.New(unsafeStr) // want `message argument is not a constant expression`
	_ = errors.New(unsafeStr /*nolint:fmtsafe*/)
}
