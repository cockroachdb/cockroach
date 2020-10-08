// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgnotice

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// Notice is an wrapper around errors that are intended to be notices.
type Notice error

// Newf generates a Notice with a format string.
func Newf(format string, args ...interface{}) Notice {
	err := errors.NewWithDepthf(1, format, args...)
	err = pgerror.WithCandidateCode(err, pgcode.SuccessfulCompletion)
	err = pgerror.WithSeverity(err, "NOTICE")
	return Notice(err)
}

// NewWithSeverityf generates a Notice with a format string and severity.
func NewWithSeverityf(severity string, format string, args ...interface{}) Notice {
	err := errors.NewWithDepthf(1, format, args...)
	err = pgerror.WithCandidateCode(err, pgcode.SuccessfulCompletion)
	err = pgerror.WithSeverity(err, severity)
	return Notice(err)
}
