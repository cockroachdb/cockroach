// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
