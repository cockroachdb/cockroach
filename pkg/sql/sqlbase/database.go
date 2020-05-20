// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// MakeDatabaseDesc constructs a DatabaseDescriptor from an AST node.
func MakeDatabaseDesc(p *tree.CreateDatabase) DatabaseDescriptor {
	return DatabaseDescriptor{
		Name:       string(p.Name),
		Privileges: NewDefaultPrivilegeDescriptor(),
	}
}
