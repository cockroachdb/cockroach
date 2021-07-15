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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/errors"
)

// QueryFn is the type of functions produced by MakeQuery.
type QueryFn func(conn Conn) (rows Rows, isMultiStatementQuery bool, err error)

// MakeQuery encapsulates a SQL query and its parameter into a
// function that can be applied to a connection object.
func MakeQuery(query string, parameters ...driver.Value) QueryFn {
	return func(conn Conn) (Rows, bool, error) {
		isMultiStatementQuery, _ := scanner.HasMultipleStatements(query)
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
		err = handleCopyError(conn.(*sqlConn), err)
		return rows, isMultiStatementQuery, err
	}
}

// handleCopyError ensures the user is properly informed when they issue
// a COPY statement somewhere in their input.
func handleCopyError(conn *sqlConn, err error) error {
	if err == nil {
		return nil
	}

	if !strings.HasPrefix(err.Error(), "pq: unknown response for simple query: 'G'") {
		return err
	}

	// The COPY statement has hosed the connection by putting the
	// protocol in a state that lib/pq cannot understand any more. Reset
	// it.
	_ = conn.Close()
	conn.reconnecting = true
	return errors.New("woops! COPY has confused this client! Suggestion: use 'psql' for COPY")
}
