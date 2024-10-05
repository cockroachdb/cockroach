// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
)

var insightPool = sync.Pool{
	New: func() interface{} {
		return new(Insight)
	},
}

func makeInsight(sessionID clusterunique.ID, transaction *Transaction) *Insight {
	insight := insightPool.Get().(*Insight)
	*insight = Insight{
		Session:     Session{ID: sessionID},
		Transaction: transaction,
	}
	return insight
}

func releaseInsight(insight *Insight) {
	insight.Statements = insight.Statements[:0]
	*insight = Insight{Statements: insight.Statements}
	insightPool.Put(insight)
}
