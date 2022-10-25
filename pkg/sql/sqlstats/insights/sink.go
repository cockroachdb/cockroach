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

type sink interface {
	AddInsight(*Insight)
}

type compositeSink struct {
	sinks []sink
}

func (c *compositeSink) AddInsight(insight *Insight) {
	for _, s := range c.sinks {
		s.AddInsight(insight)
	}
}

var _ sink = &compositeSink{}
