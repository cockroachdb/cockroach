// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

type sampledQueryLogSpy struct {
	testState          *testing.T
	stripRedactMarkers bool

	mu struct {
		syncutil.RWMutex
		logs   []eventpb.SampledQuery
		filter func(entry eventpb.SampledQuery) bool
	}
}

func (s *sampledQueryLogSpy) Intercept(entry []byte) {
	var logEntry logpb.Entry

	if err := json.Unmarshal(entry, &logEntry); err != nil {
		s.testState.Fatal(err)
	}

	if logEntry.Channel != logpb.Channel_TELEMETRY || !strings.Contains(logEntry.Message, "sampled_query") {
		return
	}

	var statementLog eventpb.SampledQuery
	if err := json.Unmarshal([]byte(logEntry.Message[logEntry.StructuredStart:logEntry.StructuredEnd]),
		&statementLog); err != nil {
		s.testState.Fatal(err)
	}

	if s.mu.filter != nil && !s.mu.filter(statementLog) {
		return
	}

	if s.stripRedactMarkers {
		statementLog.Statement = redact.RedactableString(statementLog.Statement.StripMarkers())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = append(s.mu.logs, statementLog)
}

func (s *sampledQueryLogSpy) getStatementLogs() []eventpb.SampledQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stmtLogs := make([]eventpb.SampledQuery, len(s.mu.logs))
	copy(stmtLogs, s.mu.logs)
	return stmtLogs
}

func (s *sampledQueryLogSpy) clearCollectedLogs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = make([]eventpb.SampledQuery, 0)
}
