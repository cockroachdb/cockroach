// Copyright 2016 The Cockroach Authors.
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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

type values struct {
	rows sqlbase.EncDatumRows
}

func (v *values) Next() (sqlbase.EncDatumRow, error) {
	if len(v.rows) == 0 {
		return nil, nil
	}
	row := v.rows[0]
	v.rows = v.rows[1:]
	return row, nil
}

func (v *values) Add(row sqlbase.EncDatumRow) error {
	v.rows = append(v.rows, row)
	return nil
}
