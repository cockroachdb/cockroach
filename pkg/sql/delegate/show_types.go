// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func (d *delegator) delegateShowTypes() (tree.Statement, error) {
	// TODO (SQL Features, SQL Exec): Once more user defined types are added
	//  they should be added here.
	return parse(`
SELECT
  schema, name, owner
FROM
  [SHOW ENUMS]
ORDER BY
  (schema, name)`)
}
