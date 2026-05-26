// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

func CreateDatabase(b BuildCtx, n *tree.CreateDatabase) {
	// TODO (xiang): Remove the fallback cases.
	fallBackCreateDatabaseIfMultiRegion(b, n)

	// 1. Run a bunch of pre-checks.
	createDatabasePreChecks(b, n)
	b.IncrementSchemaChangeCreateCounter("database")

	// 2. Ensure database name is not already in use.
	dbName := string(n.Name)
	if databaseExists(b, n.Name) {
		if n.IfNotExists {
			return
		}
		panic(sqlerrors.NewDatabaseAlreadyExistsError(dbName))
	}

	// 3. Ensure current user can create a database with possibly a
	// different user as the database's owner.
	owner := getOwnerOfDB(b, n)
	if err := checkCreateDatabaseCanAlterToOwner(b, n, owner); err != nil {
		panic(err)
	}

	// 4. Construct and add all relevant elements, including the database,
	// its public schema, and others.
	dbElem, _ := addCreateDatabaseElements(b, dbName, owner)
	b.LogEventForExistingTarget(dbElem)
}

func addCreateDatabaseElements(
	b BuildCtx, dbName string, dbOwner username.SQLUsername,
) (*scpb.Database, *scpb.Schema) {
	databaseID := b.GenerateUniqueDescID()
	publicSchemaID := b.GenerateUniqueDescID()

	// Add database related elements.
	dbElem := &scpb.Database{DatabaseID: databaseID}
	b.Add(dbElem)
	b.Add(&scpb.Namespace{
		DatabaseID:   0,
		SchemaID:     0,
		DescriptorID: databaseID,
		Name:         dbName,
	})
	b.Add(&scpb.DatabaseData{DatabaseID: databaseID})
	b.Add(&scpb.Owner{
		DescriptorID: databaseID,
		Owner:        dbOwner.Normalized(),
	})
	for _, up := range catpb.NewBaseDatabasePrivilegeDescriptor(dbOwner).Users {
		b.Add(&scpb.UserPrivileges{
			DescriptorID:    databaseID,
			UserName:        up.User().Normalized(),
			Privileges:      up.Privileges,
			WithGrantOption: up.WithGrantOption,
		})
	}

	// Add public schema related elements.
	scElem := &scpb.Schema{
		SchemaID: publicSchemaID,
		IsPublic: true,
	}
	b.Add(scElem)
	b.Add(&scpb.Namespace{
		DatabaseID:   databaseID,
		SchemaID:     0,
		DescriptorID: publicSchemaID,
		Name:         catconstants.PublicSchemaName,
	})
	b.Add(&scpb.SchemaParent{
		SchemaID:         publicSchemaID,
		ParentDatabaseID: databaseID,
	})
	b.Add(&scpb.Owner{
		DescriptorID: publicSchemaID,
		Owner:        dbOwner.Normalized(),
	})
	includeCreatePriv := sqlclustersettings.PublicSchemaCreatePrivilegeEnabled.Get(&b.ClusterSettings().SV)
	for _, up := range catpb.NewPublicSchemaPrivilegeDescriptor(dbOwner, includeCreatePriv).Users {
		b.Add(&scpb.UserPrivileges{
			DescriptorID:    publicSchemaID,
			UserName:        up.User().Normalized(),
			Privileges:      up.Privileges,
			WithGrantOption: up.WithGrantOption,
		})
	}

	return dbElem, scElem
}

// checkCreateDatabaseCanAlterToOwner ensures that current user can alter the ownership
// of the to-be-created database `n` to `newOwner`.
// It can if
// - current user is a super user, or
// - current user is a direct or indirect member of the new owning role.
func checkCreateDatabaseCanAlterToOwner(
	b BuildCtx, n *tree.CreateDatabase, newOwner username.SQLUsername,
) error {
	// Ensure `newOwner` exists.
	err := b.CheckRoleExists(b, newOwner)
	if err != nil {
		return err
	}
	// Ensure current user is either an admin or a member of new owing role.
	if !b.CurrentUserHasAdminOrIsMemberOf(newOwner) {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "must be member of role %q", newOwner)
	}
	return nil
}

// fallBackCreateDatabaseIfMultiRegion falls back if
//   - it's explicitly requested to be multi-region, or
//   - it has a non-empty default primary region, or
//   - the system database is multi-region, and thus it will need to inherit
//     that multi-region configuration.
func fallBackCreateDatabaseIfMultiRegion(b BuildCtx, n *tree.CreateDatabase) {
	if len(n.Regions) > 0 || n.PrimaryRegion != "" || n.SecondaryRegion != "" ||
		n.SurvivalGoal != tree.SurvivalGoalDefault || n.SuperRegion.Name != "" {
		panic(scerrors.NotImplementedError(n))
	}
	defaultPrimaryRegion := sqlclustersettings.DefaultPrimaryRegion.Get(&b.ClusterSettings().SV)
	if defaultPrimaryRegion != "" {
		panic(scerrors.NotImplementedError(n))
	}
	_, _, elem := scpb.FindDatabaseRegionConfig(b.QueryByID(keys.SystemDatabaseID))
	if elem != nil {
		panic(scerrors.NotImplementedError(n))
	}
}

// createDatabasePreChecks includes a bunch of quick pre-checks.
// It panics if any checks fails.
// The logics are copied from legacy schema changer.
func createDatabasePreChecks(b BuildCtx, n *tree.CreateDatabase) {
	if n.Name == "" {
		panic(sqlerrors.ErrEmptyDatabaseName)
	}

	if tmpl := n.Template; tmpl != "" {
		// See https://www.postgresql.org/docs/current/static/manage-ag-templatedbs.html
		if !strings.EqualFold(tmpl, "template0") {
			panic(unimplemented.NewWithIssuef(10151,
				"unsupported template: %s", tmpl))
		}
	}

	if enc := n.Encoding; enc != "" {
		// We only support UTF8 (and aliases for UTF8).
		if !(strings.EqualFold(enc, "UTF8") ||
			strings.EqualFold(enc, "UTF-8") ||
			strings.EqualFold(enc, "UNICODE")) {
			panic(unimplemented.NewWithIssueDetailf(35882, "create.db.encoding",
				"unsupported encoding: %s", enc))
		}
	}

	if col := n.Collate; col != "" {
		// We only support C and C.UTF-8.
		if col != "C" && col != "C.UTF-8" {
			panic(unimplemented.NewWithIssueDetailf(16618, "create.db.collation",
				"unsupported collation: %s", col))
		}
	}

	if ctype := n.CType; ctype != "" {
		// We only support C and C.UTF-8.
		if ctype != "C" && ctype != "C.UTF-8" {
			panic(unimplemented.NewWithIssueDetailf(35882, "create.db.classification",
				"unsupported character classification: %s", ctype))
		}
	}

	if n.ConnectionLimit != -1 {
		panic(unimplemented.NewWithIssueDetailf(
			54241,
			"create.db.connection_limit",
			"only connection limit -1 is supported, got: %d",
			n.ConnectionLimit,
		))
	}

	if n.SurvivalGoal != tree.SurvivalGoalDefault &&
		n.PrimaryRegion == tree.PrimaryRegionNotSpecifiedName {
		panic(pgerror.New(
			pgcode.InvalidDatabaseDefinition,
			"PRIMARY REGION must be specified when using SURVIVE",
		))
	}

	if n.Placement != tree.DataPlacementUnspecified {
		if !b.EvalCtx().SessionData().PlacementEnabled {
			panic(errors.WithHint(pgerror.New(
				pgcode.ExperimentalFeature,
				"PLACEMENT requires that the session setting enable_multiregion_placement_policy "+
					"is enabled",
			),
				"to use PLACEMENT, enable the session setting with SET"+
					" enable_multiregion_placement_policy = true or enable the cluster setting"+
					" sql.defaults.multiregion_placement_policy.enabled",
			))
		}

		if n.PrimaryRegion == tree.PrimaryRegionNotSpecifiedName {
			panic(pgerror.New(
				pgcode.InvalidDatabaseDefinition,
				"PRIMARY REGION must be specified when using PLACEMENT",
			))
		}

		if n.Placement == tree.DataPlacementRestricted &&
			n.SurvivalGoal == tree.SurvivalGoalRegionFailure {
			panic(pgerror.New(
				pgcode.InvalidDatabaseDefinition,
				"PLACEMENT RESTRICTED can only be used with SURVIVE ZONE FAILURE",
			))
		}
	}

	if err := canCreateDatabase(b); err != nil {
		panic(err)
	}

	if n.PrimaryRegion == tree.PrimaryRegionNotSpecifiedName && n.SecondaryRegion != tree.SecondaryRegionNotSpecifiedName {
		panic(pgerror.New(
			pgcode.InvalidDatabaseDefinition,
			"PRIMARY REGION must be specified when using SECONDARY REGION",
		))
	}

	if n.PrimaryRegion != tree.PrimaryRegionNotSpecifiedName && n.SecondaryRegion == n.PrimaryRegion {
		panic(pgerror.New(
			pgcode.InvalidDatabaseDefinition,
			"SECONDARY REGION can not be the same as the PRIMARY REGION",
		))
	}

	// Disallow CREATEs for system tenant.
	if err := shouldRestrictAccessToSystemInterface(b,
		"DDL execution",   /* operation */
		"running the DDL", /* alternate action */
	); err != nil {
		panic(err)
	}
}

// canCreateDatabase returns nil if current user has CREATEDB system privilege
// or the equivalent, legacy role options.
func canCreateDatabase(b BuildCtx) error {
	hasCreateDB, err := b.HasGlobalPrivilegeOrRoleOption(b, privilege.CREATEDB)
	if err != nil {
		return err
	}
	if !hasCreateDB {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"permission denied to create database",
		)
	}
	return nil
}

// databaseExists returns true if `dbName` has already been used by some database.
func databaseExists(b BuildCtx, dbName tree.Name) bool {
	dbElem := b.ResolveDatabase(dbName, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.CONNECT,
		RequireOwnership:    false,
		WithOffline:         true, /* We search schema with name `schema`, including offline ones. */
	})
	return dbElem != nil
}

// getOwnerOfDB either uses current user as owner of the to-be-created database,
// or the one specified in `n`.
func getOwnerOfDB(b BuildCtx, n *tree.CreateDatabase) username.SQLUsername {
	var err error
	owner := b.SessionData().User()
	if !n.Owner.Undefined() {
		owner, err = decodeusername.FromRoleSpec(
			b.SessionData(), username.PurposeValidation, n.Owner,
		)
		if err != nil {
			panic(err)
		}
	}
	return owner
}
