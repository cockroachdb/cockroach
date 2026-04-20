// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFuncSourceLister(t *testing.T) {
	defer leaktest.AfterTest(t)()

	lister := &FuncSourceLister{
		NodeSourcesFn:  func() []string { return []string{"1", "2", "3"} },
		StoreSourcesFn: func() []string { return []string{"10", "20"} },
	}

	require.Equal(t, []string{"1", "2", "3"}, lister.NodeSources())
	require.Equal(t, []string{"10", "20"}, lister.StoreSources())
}
