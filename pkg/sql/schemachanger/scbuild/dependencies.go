// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
)

// Dependencies contains all the external dependencies required by the scbuild
// package and its children.
type Dependencies interface {
	scbuildstmt.ClusterAndSessionInfo
	scbuildstmt.Telemetry

	// CatalogReader returns a CatalogReader implementation.
	CatalogReader() CatalogReader

	// TableReader returns a TableReader implementation.
	TableReader() TableReader

	// AuthorizationAccessor returns an AuthorizationAccessor implementation.
	AuthorizationAccessor() AuthorizationAccessor

	// ClusterID returns the ID of the cluster.
	// So far this is used only to build a eval.Context, for the purpose
	// of checking whether CCL features are enabled.
	ClusterID() uuid.UUID

	// Codec returns the current session data, as in execCfg.
	// So far this is used only to build a eval.Context.
	Codec() keys.SQLCodec

	// Statements returns the statements behind this schema change.
	Statements() []string

	// AstFormatter returns something that can format AST nodes.
	AstFormatter() AstFormatter

	// FeatureChecker returns something that checks schema feature flags.
	FeatureChecker() FeatureChecker

	// IndexPartitioningCCLCallback returns the CCL callback for creating
	// partitioning descriptors for indexes.
	IndexPartitioningCCLCallback() CreatePartitioningCCLCallback

	// DescriptorCommentCache returns a CommentCache
	// Implementation.
	DescriptorCommentCache() CommentCache

	// ZoneConfigGetter returns a zone config reader.
	ZoneConfigGetter() ZoneConfigGetter

	ClientNoticeSender() eval.ClientNoticeSender
}

// CreatePartitioningCCLCallback is the type of the CCL callback for creating
// partitioning descriptors for indexes.
type CreatePartitioningCCLCallback func(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *eval.Context,
	columnLookupFn func(tree.Name) (catalog.Column, error),
	oldNumImplicitColumns int,
	oldKeyColumnNames []string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning catpb.PartitioningDescriptor, err error)

// CatalogReader should implement descriptor resolution, namespace lookups, and
// all such catalog read operations for the builder. The following contract must
// apply:
// - errors are panicked;
// - caches are avoided at all times, we read straight from storage;
// - MayResolve* methods return zero values if nothing could be found;
// - MayResolve* methods ignore dropped or offline descriptors;
// - MustReadDescriptor does not;
// - MustReadDescriptor panics if the descriptor was not found.
type CatalogReader interface {
	tree.TypeReferenceResolver
	tree.QualifiedNameResolver
	tree.FunctionReferenceResolver

	// MayResolveDatabase looks up a database by name.
	MayResolveDatabase(ctx context.Context, name tree.Name) catalog.DatabaseDescriptor

	// MayResolveSchema looks up a schema by name.
	MayResolveSchema(ctx context.Context, name tree.ObjectNamePrefix) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor)

	// MayResolveTable looks up a table by name.
	MayResolveTable(ctx context.Context, name tree.UnresolvedObjectName) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// MayResolveType looks up a type by name.
	MayResolveType(ctx context.Context, name tree.UnresolvedObjectName) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor)

	// MayResolveIndex looks up a table containing index with provided index name.
	// The resolving contract is that:
	// (1) if table name is provided, table is resolved and index is searched within the table.
	// (2) if table name is not given, but schema name (in CRDB, db name can be
	// the schema name here) is present, schema is resolved first, then all tables
	// are looped to searched for the index.
	// (3) if only index name is present, all tables in all schemas on current
	// search path are looped to look up the index.
	// It's possible that index does not exist, in which case it won't panic but
	// "found=false" is returned.
	MayResolveIndex(ctx context.Context, tableIndexName tree.TableIndexName) (
		found bool, prefix catalog.ResolvedObjectPrefix, tbl catalog.TableDescriptor, idx catalog.Index,
	)

	// ReadObjectNamesAndIDs looks up the namespace entries for a schema.
	ReadObjectNamesAndIDs(ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor) (tree.TableNames, descpb.IDs)

	// MustReadDescriptor looks up a descriptor by ID.
	MustReadDescriptor(ctx context.Context, id descpb.ID) catalog.Descriptor

	// MustGetSchemasForDatabase gets schemas associated with
	// a database.
	MustGetSchemasForDatabase(
		ctx context.Context, database catalog.DatabaseDescriptor,
	) map[descpb.ID]string
}

// TableReader implements functions for inspecting tables during the build phase,
// checking their contents for example to determine if they are empty.
type TableReader interface {
	// IsTableEmpty returns if the table is empty.
	IsTableEmpty(ctx context.Context, id descpb.ID, primaryIndexID descpb.IndexID) bool
}

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the current user has `privilege` on
	// `descriptor`.
	CheckPrivilege(
		ctx context.Context, privilegeObject catalog.PrivilegeObject, privilege privilege.Kind,
	) error

	// HasAdminRole verifies if a user has an admin role.
	HasAdminRole(ctx context.Context) (bool, error)

	// HasOwnership returns true iff the role, or any role the role is a member
	// of, has ownership privilege of the desc.
	HasOwnership(ctx context.Context, privilegeObject catalog.PrivilegeObject) (bool, error)

	// CheckPrivilegeForUser verifies that the user has `privilege` on `descriptor`.
	CheckPrivilegeForUser(
		ctx context.Context, privilegeObject catalog.PrivilegeObject, privilege privilege.Kind, user username.SQLUsername,
	) error

	// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct
	// and indirect) and returns a map of "role" -> "isAdmin".
	MemberOfWithAdminOption(ctx context.Context, member username.SQLUsername) (map[username.SQLUsername]bool, error)
}

// AstFormatter provides interfaces for formatting AST nodes.
type AstFormatter interface {
	// FormatAstAsRedactableString formats a tree.Statement into SQL with fully
	// qualified names, where parts can be redacted.
	FormatAstAsRedactableString(statement tree.Statement, annotations *tree.Annotations) redact.RedactableString
}

// CommentCache represent an interface to fetch and cache comments for
// descriptors.
type CommentCache interface {
	scdecomp.CommentGetter

	// LoadCommentsForObjects explicitly loads commentCache into the cache give a list
	// of object id of a descriptor type.
	LoadCommentsForObjects(ctx context.Context, objIDs []descpb.ID) error
}

// ZoneConfigGetter see scdecomp.ZoneConfigGetter
type ZoneConfigGetter scdecomp.ZoneConfigGetter

// SchemaResolverFactory is used to construct a new schema resolver with
// injected dependencies.
type SchemaResolverFactory func(
	descCollection *descs.Collection,
	sessionDataStack *sessiondata.Stack,
	txn *kv.Txn,
	authAccessor AuthorizationAccessor,
) resolver.SchemaResolver
