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
	"database/sql/driver"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// QueryFn is the type of functions produced by MakeQuery.
type QueryFn func(conn Conn) (rows Rows, isMultiStatementQuery bool, err error)

// MakeQuery encapsulates a SQL query and its parameter into a
// function that can be applied to a connection object.
func MakeQuery(query string, parameters ...driver.Value) QueryFn {
	return func(conn Conn) (Rows, bool, error) {
		isMultiStatementQuery := parser.HasMultipleStatements(query)
		// driver.Value is an alias for interface{}, but must adhere to a restricted
		// set of types when being passed to driver.Queryer.Query (see
		// driver.IsValue). We use driver.DefaultParameterConverter to perform the
		// necessary conversion. This is usually taken care of by the sql package,
		// but we have to do so manually because we're talking directly to the
		// driver.
		for i := range parameters {
			var err error
			parameters[i], err = driver.DefaultParameterConverter.ConvertValue(parameters[i])
			if err != nil {
				return nil, isMultiStatementQuery, err
			}
		}
		rows, err := conn.Query(query, parameters)
		return rows, isMultiStatementQuery, err
	}
}
