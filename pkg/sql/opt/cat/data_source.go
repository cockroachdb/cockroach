// Copyright 2018 The Cockroach Authors.
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

package cat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// DataSourceName is an alias for tree.TableName, and is used for views and
// sequences as well as tables.
type DataSourceName = tree.TableName

// DataSource is an interface to a database object that provides rows, like a
// table, a view, or a sequence.
type DataSource interface {
	Object

	// Version uniquely identifies a particular iteration of the data source's
	// schema. Each time the schema changes, the version will be incremented,
	// which allows changes to be easily detected. See the comment for the Version
	// type for more detail.
	Version() Version

	// Name returns the fully normalized, fully qualified, and fully resolved
	// name of the data source (<db-name>.<schema-name>.<data-source-name>). The
	// ExplicitCatalog and ExplicitSchema fields will always be true, since all
	// parts of the name are always specified.
	Name() *DataSourceName
}
