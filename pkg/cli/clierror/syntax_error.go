// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clierror

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgconn"
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
