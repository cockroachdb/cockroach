// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterSpec_Args(t *testing.T) {
	const nodeCount = 12
	const instanceType = ""
	var opts []Option
	opts = append(opts, Geo())
	s := MakeClusterSpec(AWS, instanceType, nodeCount, opts...)
	t.Logf("%#v", s)
	// Regression test against a bug in which we would request an SSD machine type
	// together with the --local-ssd=false option.
	act := fmt.Sprint(s.Args())
	require.NotContains(t, act, "--aws-machine-type-ssd")
	require.Contains(t, act, "--local-ssd=false")
}
