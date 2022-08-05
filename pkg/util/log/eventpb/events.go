// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventpb

import "github.com/cockroachdb/cockroach/pkg/util/log/logpb"

// EventWithCommonSQLPayload is implemented by CommonSQLEventDetails.
type EventWithCommonSQLPayload interface {
	logpb.EventPayload
	CommonSQLDetails() *CommonSQLEventDetails
}

// CommonSQLDetails implements the EventWithCommonSQLPayload interface.
func (m *CommonSQLEventDetails) CommonSQLDetails() *CommonSQLEventDetails { return m }

// EventWithCommonSchemaChangePayload is implemented by CommonSchemaChangeDetails.
type EventWithCommonSchemaChangePayload interface {
	logpb.EventPayload
	CommonSchemaChangeDetails() *CommonSchemaChangeEventDetails
}

// CommonSchemaChangeDetails implements the EventWithCommonSchemaChangePayload interface.
func (m *CommonSchemaChangeEventDetails) CommonSchemaChangeDetails() *CommonSchemaChangeEventDetails {
	return m
}

var _ EventWithCommonSchemaChangePayload = (*FinishSchemaChange)(nil)
var _ EventWithCommonSchemaChangePayload = (*ReverseSchemaChange)(nil)
var _ EventWithCommonSchemaChangePayload = (*FinishSchemaChangeRollback)(nil)

// EventWithCommonJobPayload is implemented by CommonSQLEventDetails.
type EventWithCommonJobPayload interface {
	logpb.EventPayload
	CommonJobDetails() *CommonJobEventDetails
}

// CommonJobDetails implements the EventWithCommonJobPayload interface.
func (m *CommonJobEventDetails) CommonJobDetails() *CommonJobEventDetails { return m }

var _ EventWithCommonJobPayload = (*Import)(nil)
var _ EventWithCommonJobPayload = (*Restore)(nil)

// RecoveryEventType describes the type of recovery for a RecoveryEvent.
type RecoveryEventType string
