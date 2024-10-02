// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpanConfigConformanceReport_IsEmpty(t *testing.T) {
	var r SpanConfigConformanceReport
	require.True(t, r.IsEmpty())
	r.ViolatingConstraints = make([]ConformanceReportedRange, 0)
	require.True(t, r.IsEmpty())
	r.ViolatingConstraints = append(r.ViolatingConstraints, ConformanceReportedRange{})
	require.False(t, r.IsEmpty())

	require.Equal(
		t, numSpanConfigConformanceReportSlices,
		reflect.TypeOf(&r).Elem().NumField(),
	)
}
