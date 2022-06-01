// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clierror

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
)

// IsSQLSyntaxError returns true iff the provided error is a SQL
// syntax error. The function works for the queries executed via the
// clisqlclient/clisqlexec packages.
func IsSQLSyntaxError(err error) bool {
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
		return pgErr.Code == pgcode.Syntax.String()
	}
	return false
}
