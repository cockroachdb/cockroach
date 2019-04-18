// Copyright 2019 The Cockroach Authors.
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

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowSequences returns all the schemas in the given or current database.
// Privileges: None.
//   Notes: postgres does not have a SHOW SEQUENCES statement.
func (d *delegator) delegateShowSequences(n *tree.ShowSequences) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}

	getSequencesQuery := fmt.Sprintf(`
	  SELECT sequence_name
	    FROM %[1]s.information_schema.sequences
	   WHERE sequence_catalog = %[2]s
	     AND sequence_schema = 'public'
	ORDER BY sequence_name`,
		name.String(), // note: (tree.Name).String() != string(name)
		lex.EscapeSQLString(string(name)),
	)
	return parse(getSequencesQuery)
}
