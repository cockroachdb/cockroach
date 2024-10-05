// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
