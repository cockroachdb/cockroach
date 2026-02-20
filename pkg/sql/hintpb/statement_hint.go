// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hintpb

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// HintTypeEmpty is the default value, used if the hint type cannot be
	// determined.
	HintTypeEmpty = "EMPTY"
	// HintTypeRewriteInlineHints is used for "hint injection" hints that rewrite
	// the inline hints within the AST of a statement.
	HintTypeRewriteInlineHints = "REWRITE INLINE HINTS"
)

// HintTypeStr returns the string representation of the type of the given hint,
// suitable for use in the statement_hints table.
func (hint *StatementHintUnion) HintTypeStr() string {
	switch hint.GetValue().(type) {
	case *InjectHints:
		return HintTypeRewriteInlineHints
	default:
		return HintTypeEmpty
	}
}

// FromBytes converts the raw bytes from system.statement_hints into a
// StatementHintUnion object.
func FromBytes(bytes []byte) (StatementHintUnion, error) {
	res := StatementHintUnion{}
	if err := protoutil.Unmarshal(bytes, &res); err != nil {
		return StatementHintUnion{}, err
	}
	if res.GetValue() == nil {
		return StatementHintUnion{}, errors.New("invalid hint bytes: no value set")
	}
	return res, nil
}

// ParseHintProto unmarshals raw hint protobuf bytes, guarding against panics.
// Returns the deserialized StatementHintUnion, or an error if unmarshaling
// fails.
func ParseHintProto(hintBytes []byte) (hint StatementHintUnion, retErr error) {
	defer errorutil.MaybeCatchPanic(&retErr, nil /* errCallback */)
	hint, retErr = FromBytes(hintBytes)
	return hint, retErr
}

// ToBytes converts the StatementHintUnion to a raw bytes representation that
// can be inserted into the system.statement_hints table.
func ToBytes(hint StatementHintUnion) ([]byte, error) {
	if hint.GetValue() == nil {
		return nil, errors.New("cannot convert empty hint to bytes")
	}
	return protoutil.Marshal(&hint)
}

// HintType returns a string representation of the hint type for display.
func (h StatementHintUnion) HintType() string {
	switch h.GetValue().(type) {
	case *InjectHints:
		return "rewrite_inline_hints"
	default:
		return "unknown"
	}
}

// RecreateStmt returns the SQL statement that can be used to recreate the hint.
// Returns the empty string and false if the hint type is not supported.
func (h StatementHintUnion) RecreateStmt(stmt string) (string, bool) {
	switch t := h.GetValue().(type) {
	case *InjectHints:
		return fmt.Sprintf(
			"SELECT information_schema.crdb_rewrite_inline_hints(%s, %s);",
			lexbase.EscapeSQLString(stmt),
			lexbase.EscapeSQLString(t.DonorSQL),
		), true
	default:
		return "", false
	}
}

// Details returns a JSON representation of the hint details. This is used for
// displaying hint information in SHOW STATEMENT HINTS WITH DETAILS.
func (h *StatementHintUnion) Details() (json.JSON, error) {
	var wrapped protoutil.Message
	switch t := h.GetValue().(type) {
	case *InjectHints:
		wrapped = t
	default:
		return nil, errors.New("unknown hint type")
	}
	flags := protoreflect.FmtFlags{EmitDefaults: true}
	return protoreflect.MessageToJSON(wrapped, flags)
}
