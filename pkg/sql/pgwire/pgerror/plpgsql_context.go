// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgerror

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/gogo/protobuf/proto"
)

// WithPLpgSQLContext decorates the error with a PLpgSQL context.
func WithPLpgSQLContext(err error, plpgsqlContext string) error {
	if err == nil {
		return nil
	}

	return &withPLpgSQLContext{cause: err, plpgsqlContext: plpgsqlContext}
}

// GetPLpgSQLContext attempts to unwrap and find a PLpgSQL context.
func GetPLpgSQLContext(err error) string {
	if c := (*withPLpgSQLContext)(nil); errors.As(err, &c) {
		return c.plpgsqlContext
	}
	return ""
}

type withPLpgSQLContext struct {
	cause          error
	plpgsqlContext string
}

var _ error = (*withPLpgSQLContext)(nil)
var _ errors.SafeDetailer = (*withPLpgSQLContext)(nil)
var _ fmt.Formatter = (*withPLpgSQLContext)(nil)
var _ errors.SafeFormatter = (*withPLpgSQLContext)(nil)

func (w *withPLpgSQLContext) Error() string { return w.cause.Error() }
func (w *withPLpgSQLContext) Cause() error  { return w.cause }
func (w *withPLpgSQLContext) Unwrap() error { return w.cause }
func (w *withPLpgSQLContext) SafeDetails() []string {
	// The PLpgSQL context may contain function names which are considered PII.
	return nil
}

func (w *withPLpgSQLContext) Format(s fmt.State, verb rune) { errors.FormatError(w, s, verb) }

func (w *withPLpgSQLContext) SafeFormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf("PLpgSQL context: %s", w.plpgsqlContext)
	}
	return w.cause
}

// encodeWithPLpgSQLContext is a custom encoder for withPLpgSQLContext errors.
// Note that as the last return value it uses proto.Message (and not
// protoutil.Message which is required by linter) because the latter brings in
// additional dependencies into this package and the former is sufficient here.
func encodeWithPLpgSQLContext(_ context.Context, err error) (string, []string, proto.Message) {
	w := err.(*withPLpgSQLContext)
	return "", nil, &errorspb.StringPayload{Msg: w.plpgsqlContext}
}

func decodeWithPLpgSQLContext(
	_ context.Context, cause error, _ string, _ []string, payload proto.Message,
) error {
	m, ok := payload.(*errorspb.StringPayload)
	if !ok {
		return nil
	}
	return &withPLpgSQLContext{cause: cause, plpgsqlContext: m.Msg}
}

func init() {
	key := errors.GetTypeKey((*withPLpgSQLContext)(nil))
	errors.RegisterWrapperEncoder(key, encodeWithPLpgSQLContext)
	errors.RegisterWrapperDecoder(key, decodeWithPLpgSQLContext)
}
