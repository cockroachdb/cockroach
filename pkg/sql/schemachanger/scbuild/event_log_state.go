// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

var _ scbuildstmt.EventLogState = (*eventLogState)(nil)

// TargetMetadata implements the scbuildstmt.EventLogState interface.
func (e *eventLogState) TargetMetadata() scpb.TargetMetadata {
	return e.statementMetaData
}

// IncrementSubWorkID implements the scbuildstmt.EventLogState interface.
func (e *eventLogState) IncrementSubWorkID() {
	e.statementMetaData.SubWorkID++
}

// EventLogStateWithNewSourceElementID implements the scbuildstmt.EventLogState
// interface.
func (e *eventLogState) EventLogStateWithNewSourceElementID() scbuildstmt.EventLogState {
	*e.sourceElementID++
	return &eventLogState{
		statements:      e.statements,
		authorization:   e.authorization,
		sourceElementID: e.sourceElementID,
		statementMetaData: scpb.TargetMetadata{
			StatementID:     e.statementMetaData.StatementID,
			SubWorkID:       e.statementMetaData.SubWorkID,
			SourceElementID: *e.sourceElementID,
		},
	}
}
