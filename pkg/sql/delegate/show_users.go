// Copyright 2017 The Cockroach Authors.
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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (d *delegator) delegateShowUsers(n *tree.ShowUsers) (tree.Statement, error) {
	return parse(`SELECT username AS user_name FROM system.users WHERE "isRole" = false ORDER BY 1`)
}
