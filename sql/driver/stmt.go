// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import "database/sql/driver"

type stmt struct {
	conn *conn
	stmt string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	// TODO(pmattis): Count the number of parameters.
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.Exec(s.stmt, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.Query(s.stmt, args)
}
