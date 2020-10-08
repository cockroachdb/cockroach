// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type withCandidateCode struct {
	cause error
	code  string
}

var _ error = (*withCandidateCode)(nil)
var _ errors.SafeDetailer = (*withCandidateCode)(nil)
var _ fmt.Formatter = (*withCandidateCode)(nil)
var _ errors.SafeFormatter = (*withCandidateCode)(nil)

func (w *withCandidateCode) Error() string         { return w.cause.Error() }
func (w *withCandidateCode) Cause() error          { return w.cause }
func (w *withCandidateCode) Unwrap() error         { return w.cause }
func (w *withCandidateCode) SafeDetails() []string { return []string{w.code} }

func (w *withCandidateCode) Format(s fmt.State, verb rune) { errors.FormatError(w, s, verb) }

func (w *withCandidateCode) SafeFormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf("candidate pg code: %s", errors.Safe(w.code))
	}
	return w.cause
}

// decodeWithCandidateCode is a custom decoder that will be used when decoding
// withCandidateCode error objects.
// Note that as the last argument it takes proto.Message (and not
// protoutil.Message which is required by linter) because the latter brings in
// additional dependencies into this package and the former is sufficient here.
func decodeWithCandidateCode(
	_ context.Context, cause error, _ string, details []string, _ proto.Message,
) error {
	code := pgcode.Uncategorized.String()
	if len(details) > 0 {
		code = details[0]
	}
	return &withCandidateCode{cause: cause, code: code}
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*withCandidateCode)(nil)), decodeWithCandidateCode)
}
