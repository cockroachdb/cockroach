// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logpb

import (
	"reflect"
	"strings"

	"github.com/cockroachdb/redact"
)

// EventPayload is implemented by CommonEventDetails.
type EventPayload interface {
	// CommonDetails gives access to the common payload.
	CommonDetails() *CommonEventDetails
	// LoggingChannel indicates which logging channel to send this event to.
	// This is defined by the event category, at the top of each .proto file.
	LoggingChannel() Channel
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
