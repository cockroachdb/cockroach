// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/redact"
)

// inspectIssue represents a single validation failure detected by an inspectCheck.
// These issues correspond to rows written into the system.inspect_errors table.
//
// Each issue identifies the object where the inconsistency was found, the specific
// problem type, and a JSON-serializable details map with check-specific context.
type inspectIssue struct {
	// ErrorType is a machine-readable string describing the type of validation failure
	ErrorType redact.RedactableString

	// DatabaseID is the descriptor ID of the database containing the object in error.
	// May be 0 if not applicable.
	DatabaseID descpb.ID

	// SchemaID is the descriptor ID of the schema containing the object in error.
	// May be 0 if not applicable.
	SchemaID descpb.ID

	// ObjectID is the descriptor ID of the thing where the error occurred.
	// Usually this is the ID of the table.
	ObjectID descpb.ID

	// PrimaryKey is the primary key of the row involved in the issue, rendered as a string.
	// If the error does not relate to a specific row, this may be empty.
	PrimaryKey string

	// Details contains additional structured metadata describing the issue.
	// The contents vary by check type.
	Details map[redact.RedactableString]interface{}
}

var _ redact.SafeFormatter = (*inspectIssue)(nil)

// SafeFormat implements the redact.SafeFormatter interface.
func (i inspectIssue) SafeFormat(w redact.SafePrinter, _ rune) {
	var buf redact.StringBuilder
	buf.Printf("{type=")
	buf.Print(i.ErrorType)
	if i.DatabaseID != 0 {
		buf.Printf(" db=%d", i.DatabaseID)
	}
	if i.SchemaID != 0 {
		buf.Printf(" schema=%d", i.SchemaID)
	}
	if i.ObjectID != 0 {
		buf.Printf(" obj=%d", i.ObjectID)
	}
	buf.Printf(" pk=%q", i.PrimaryKey)
	if i.Details != nil {
		buf.Printf(" details=%+v", i.Details)
	}
	buf.Printf("}")
	w.Print(buf)
}

func (i inspectIssue) String() string { return redact.StringWithoutMarkers(i) }
