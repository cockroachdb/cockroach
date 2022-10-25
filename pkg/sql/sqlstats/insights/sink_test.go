// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeSink(t *testing.T) {
	one := &fakeSink{}
	two := &fakeSink{}
	s := &compositeSink{[]sink{one, two}}
	s.AddInsight(&Insight{})
	require.True(t, one.added)
	require.True(t, two.added)
}

type fakeSink struct {
	added bool
}

func (f *fakeSink) AddInsight(*Insight) {
	f.added = true
}

var _ sink = &fakeSink{}
