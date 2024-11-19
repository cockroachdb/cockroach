// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
)

// DescriptorSessionDataProvider is the union of various interfaces which
// expose descriptor-related state from session data.
type DescriptorSessionDataProvider interface {
	TemporarySchemaProvider
	DescriptorValidationModeProvider
}

// DescriptorValidationModeProvider exports the same interface in catkv.
type DescriptorValidationModeProvider = catkv.DescriptorValidationModeProvider

// TemporarySchemaProvider encapsulates the temporary schema information in
// the current session data.
type TemporarySchemaProvider interface {

	// HasTemporarySchema returns false iff the there is no temporary schemas in
	// the underlying session data.
	HasTemporarySchema() bool

	// GetTemporarySchemaName returns the name of the temporary schema for this
	// session according to the search path, if it exists.
	GetTemporarySchemaName() string

	// GetTemporarySchemaIDForDB returns the ID of the temporary schema for this
	// session for the given database ID.
	GetTemporarySchemaIDForDB(dbID descpb.ID) descpb.ID

	// MaybeGetDatabaseForTemporarySchemaID returns the ID of the parent database
	// of the temporary schema with the given ID, if it exists, 0 otherwise.
	MaybeGetDatabaseForTemporarySchemaID(id descpb.ID) descpb.ID
}
