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
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// This registry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// exposes the set of currently retained insights.
type lockingRegistry struct {
	detector detector
	causes   *causes
	store    *lockingStore

	// Note that this single mutex places unnecessary constraints on outlier
	// detection and reporting. We will develop a higher-throughput system
	// before enabling the insights subsystem by default.
	mu struct {
		syncutil.RWMutex
		statements map[clusterunique.ID]*statementBuf
	}
}

var _ Writer = &lockingRegistry{}
var _ Reader = &lockingRegistry{}

func (r *lockingRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.mu.statements[sessionID]
	if !ok {
		b = statementsBufPool.Get().(*statementBuf)
		r.mu.statements[sessionID] = b
	}
	b.append(statement)
}

type statementBuf []*Statement

func (b *statementBuf) append(statement *Statement) {
	*b = append(*b, statement)
}

func (b *statementBuf) release() {
	for i, n := 0, len(*b); i < n; i++ {
		(*b)[i] = nil
	}
	*b = (*b)[:0]
	statementsBufPool.Put(b)
}

var statementsBufPool = sync.Pool{
	New: func() interface{} {
		return new(statementBuf)
	},
}

func (r *lockingRegistry) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	if !r.enabled() {
		return
	}
	statements := r.popSessionStatements(sessionID)
	defer statements.release()

	var slowStatements util.FastIntSet
	for i, s := range *statements {
		if r.detector.isSlow(s) {
			slowStatements.Add(i)
		}
	}
	if slowStatements.Empty() {
		return
	}
	// Note that we'll record insights for every statement, not just for
	// the slow ones.
	for i, s := range *statements {
		insight := makeInsight(sessionID, transaction, s)
		if slowStatements.Contains(i) {
			switch s.Status {
			case Statement_Completed:
				insight.Problem = Problem_SlowExecution
				insight.Causes = r.causes.examine(insight.Causes, s)
			case Statement_Failed:
				// Note that we'll be building better failure support for 23.1.
				// For now, we only mark failed statements that were also slow.
				insight.Problem = Problem_FailedExecution
			}
		}
		r.store.AddInsight(insight)
	}
}

func (r *lockingRegistry) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	r.store.IterateInsights(ctx, visitor)
}

// TODO(todd):
//
//	Once we can handle sufficient throughput to live on the hot
//	execution path in #81021, we can probably get rid of this external
//	concept of "enabled" and let the detectors just decide for themselves
//	internally.
func (r *lockingRegistry) enabled() bool {
	return r.detector.enabled()
}

func (r *lockingRegistry) popSessionStatements(sessionID clusterunique.ID) *statementBuf {
	r.mu.Lock()
	defer r.mu.Unlock()
	statements := r.mu.statements[sessionID]
	delete(r.mu.statements, sessionID)
	return statements
}

func newRegistry(st *cluster.Settings, detector detector) *lockingRegistry {
	r := &lockingRegistry{
		detector: detector,
		causes:   &causes{st: st},
		store:    newStore(st),
	}
	r.mu.statements = make(map[clusterunique.ID]*statementBuf)
	return r
}
