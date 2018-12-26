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

// Schema is an interface to a database schema, which is a namespace that
// contains other database objects, like tables and views. Examples of schema
// are "public" and "crdb_internal".
type Schema interface {
	Object

	// Name returns the fully normalized, fully qualified, and fully resolved
	// name of the schema (<db-name>.<schema-name>). The ExplicitCatalog
	// and ExplicitSchema fields will always be true, since all parts of the
	// name are always specified.
	Name() *SchemaName
}
