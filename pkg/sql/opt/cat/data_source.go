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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// DataSourceName is an alias for tree.TableName, and is used for views and
// sequences as well as tables.
type DataSourceName = tree.TableName

// DataSource is an interface to a database object that provides rows, like a
// table, a view, or a sequence.
type DataSource interface {
	Object

	// Name returns the fully normalized, fully qualified, and fully resolved
	// name of the data source (<db-name>.<schema-name>.<data-source-name>). The
	// ExplicitCatalog and ExplicitSchema fields will always be true, since all
	// parts of the name are always specified.
	Name() *DataSourceName
}
