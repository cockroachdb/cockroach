// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// See grunning.Supported() for an explanation behind this build tag.
//
//go:build freebsd || (linux && s390x) || !bazel
// +build freebsd linux,s390x !bazel

package grunning_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/stretchr/testify/require"
)

func TestDisabled(t *testing.T) {
	require.False(t, grunning.Supported())
	require.Zero(t, grunning.Time())
	require.Zero(t, grunning.Difference(grunning.Time(), grunning.Time()))
	require.Zero(t, grunning.Elapsed(grunning.Time(), grunning.Time()))
}
