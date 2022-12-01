// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
