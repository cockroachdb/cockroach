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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
)

// TODO (xinhaoz): Remove this pool (#128199). The insights object
// can use the existing statementBuf pool for the statements slice.
var insightPool = sync.Pool{
	New: func() interface{} {
		return new(Insight)
	},
}

func makeInsight(sessionID clusterunique.ID, transaction *Transaction) *Insight {
	insight := insightPool.Get().(*Insight)
	insight.Session = Session{ID: sessionID}
	insight.Transaction = transaction
	return insight
}

func releaseInsight(insight *Insight) {
	for i := range insight.Statements {
		insight.Statements[i] = nil
	}
	insight.Statements = insight.Statements[:0]
	*insight = Insight{Statements: insight.Statements}
	insightPool.Put(insight)
}
