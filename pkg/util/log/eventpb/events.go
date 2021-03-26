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

import (
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/redact"
)

// GetEventTypeName retrieves the system.eventlog type name for the given payload.
func GetEventTypeName(event EventPayload) string {
	// This logic takes the type names and converts from CamelCase to snake_case.
	typeName := reflect.TypeOf(event).Elem().Name()
	var res strings.Builder
	res.WriteByte(typeName[0] + 'a' - 'A')
	for i := 1; i < len(typeName); i++ {
		if typeName[i] >= 'A' && typeName[i] <= 'Z' {
			res.WriteByte('_')
			res.WriteByte(typeName[i] + 'a' - 'A')
		} else {
			res.WriteByte(typeName[i])
		}
	}
	return res.String()
}

// EventPayload is implemented by CommonEventDetails.
type EventPayload interface {
	// CommonDetails gives access to the common payload.
	CommonDetails() *CommonEventDetails
	// LoggingChannel indicates which logging channel to send this event to.
	// This is defined by the event category, at the top of each .proto file.
	LoggingChannel() logpb.Channel
	// AppendJSONFields appends the JSON representation of the event's
	// fields to the given redactable byte slice. Note that the
	// representation is missing the outside '{' and '}'
	// delimiters. This is intended so that the outside printer can
	// decide how to embed the event in a larger payload.
	//
	// The printComma, if true, indicates whether to print a comma
	// before the first field. The returned bool value indicates whether
	// to print a comma when appending more fields afterwards.
	AppendJSONFields(printComma bool, b redact.RedactableBytes) (bool, redact.RedactableBytes)
}

// CommonDetails implements the EventWithCommonPayload interface.
func (m *CommonEventDetails) CommonDetails() *CommonEventDetails { return m }

// EventWithCommonSQLPayload is implemented by CommonSQLEventDetails.
type EventWithCommonSQLPayload interface {
	EventPayload
	CommonSQLDetails() *CommonSQLEventDetails
}

// CommonSQLDetails implements the EventWithCommonSQLPayload interface.
func (m *CommonSQLEventDetails) CommonSQLDetails() *CommonSQLEventDetails { return m }

// EventWithCommonSchemaChangePayload is implemented by CommonSchemaChangeDetails.
type EventWithCommonSchemaChangePayload interface {
	EventPayload
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
	EventPayload
	CommonJobDetails() *CommonJobEventDetails
}

// CommonJobDetails implements the EventWithCommonJobPayload interface.
func (m *CommonJobEventDetails) CommonJobDetails() *CommonJobEventDetails { return m }

var _ EventWithCommonJobPayload = (*Import)(nil)
var _ EventWithCommonJobPayload = (*Restore)(nil)
