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

func (d *delegator) delegateShowEnums(n *tree.ShowEnums) (tree.Statement, error) {
	query := `
  SELECT type.typname AS name,
         string_agg(enum.enumlabel, '|') AS value
    FROM pg_enum AS enum
    JOIN pg_type AS type ON (type.oid = enum.enumtypid)
GROUP BY type.typname
ORDER BY type.typname`

	return parse(query)
}
