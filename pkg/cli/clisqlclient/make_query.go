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
	"database/sql/driver"

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

func convertArgs(parameters []interface{}) ([]driver.NamedValue, error) {
	dVals := make([]driver.NamedValue, len(parameters))
	for i := range parameters {
		// driver.NamedValue.Value is an alias for interface{}, but must adhere to a restricted
		// set of types when being passed to driver.Queryer.Query (see
		// driver.IsValue). We use driver.DefaultParameterConverter to perform the
		// necessary conversion. This is usually taken care of by the sql package,
		// but we have to do so manually because we're talking directly to the
		// driver.
		var err error
		dVals[i].Ordinal = i + 1
		dVals[i].Value, err = driver.DefaultParameterConverter.ConvertValue(parameters[i])
		if err != nil {
			return nil, err
		}
	}
	return dVals, nil
}
