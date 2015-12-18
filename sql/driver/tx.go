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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import "database/sql/driver"

var _ driver.Tx = tx{}

type tx struct {
	conn *conn
}

func (t tx) Commit() error {
	_, err := t.conn.Exec("COMMIT TRANSACTION", nil)
	return err
}

func (t tx) Rollback() error {
	_, err := t.conn.Exec("ROLLBACK TRANSACTION", nil)
	return err
}
