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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// This registry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// writes insights into the provided sink.
type lockingRegistry struct {
	statements map[clusterunique.ID]*statementBuf
	detector   detector
	causes     *causes
	sink       sink
}

var _ Writer = &lockingRegistry{}

func (r *lockingRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	if !r.enabled() {
		return
	}
	b, ok := r.statements[sessionID]
	if !ok {
		b = statementsBufPool.Get().(*statementBuf)
		r.statements[sessionID] = b
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
	statements := r.statements[sessionID]
	delete(r.statements, sessionID)
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
		r.sink.AddInsight(insight)
	}
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

func newRegistry(st *cluster.Settings, detector detector, sink sink) *lockingRegistry {
	return &lockingRegistry{
		statements: make(map[clusterunique.ID]*statementBuf),
		detector:   detector,
		causes:     &causes{st: st},
		sink:       sink,
	}
}
