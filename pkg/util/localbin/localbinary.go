// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package localbin

import (
	"errors"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

// GetBazelBinaryPath returns the path of a bazel generated cockroach binary.
// NB: The binary must be specified as a target to build in the BUILD.bazel.
func GetBazelBinaryPath() (string, error) {
	if bazel.BuiltWithBazel() {
		binaryPath, found := bazel.FindBinary("pkg/cmd/cockroach/cockroach_", "cockroach")
		if !found {
			return "", errors.New("BuiltWithBazel: cockroach binary not found")
		}
		return binaryPath, nil
	}

	return "", errors.New("GetBazelBinaryPath is only supported under BuiltWithBazel")
}
