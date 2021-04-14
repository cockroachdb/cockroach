// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DataSourceName is an alias for tree.TableName, and is used for views and
// sequences as well as tables.
type DataSourceName = tree.TableName

// DataSource is an interface to a database object that provides rows, like a
// table, a view, or a sequence.
type DataSource interface {
	Object

	// Name returns the unqualified name of the object.
	Name() tree.Name

	// CollectTypes returns all user defined types that the column uses.
	// This includes types used in default expressions, computed columns,
	// and the type of the column itself.
	CollectTypes(ord int) (descpb.IDs, error)
}
