// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
)

// QueryFn is the type of functions produced by MakeQuery.
type QueryFn func(ctx context.Context, conn Conn) (rows Rows, isMultiStatementQuery bool, err error)

// MakeQuery encapsulates a SQL query and its parameter into a
// function that can be applied to a connection object.
func MakeQuery(query string, parameters ...interface{}) QueryFn {
	return func(ctx context.Context, conn Conn) (Rows, bool, error) {
		isMultiStatementQuery, _ := scanner.HasMultipleStatements(query)
		rows, err := conn.Query(ctx, query, parameters...)
		return rows, isMultiStatementQuery, err
	}
}
