// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers

import "github.com/cockroachdb/cockroach/pkg/settings/cluster"

type detector interface {
	enabled() bool
	isOutlier(*Outlier_Statement) bool
}

var _ detector = &latencyThresholdDetector{}

type latencyThresholdDetector struct {
	st *cluster.Settings
}

func (l latencyThresholdDetector) enabled() bool {
	return LatencyThreshold.Get(&l.st.SV) > 0
}

func (l latencyThresholdDetector) isOutlier(s *Outlier_Statement) bool {
	return l.enabled() && s.LatencyInSeconds >= LatencyThreshold.Get(&l.st.SV).Seconds()
}
