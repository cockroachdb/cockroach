// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catconstants

const (
	// NamespaceTableFamilyID is the column family of the namespace table which is
	// actually written to.
	NamespaceTableFamilyID = 4

	// NamespaceTablePrimaryIndexID is the id of the primary index of the
	// namespace table.
	NamespaceTablePrimaryIndexID = 1

	// PreMigrationNamespaceTableName is the name that was used on the descriptor
	// of the current namespace table before the DeprecatedNamespaceTable was
	// migrated away.
	PreMigrationNamespaceTableName = "namespace2"
)
