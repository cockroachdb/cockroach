// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// Grant adds privileges to users.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// Privileges: GRANT on database/table/view.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Grant(ctx context.Context, n *tree.Grant) (planNode, error) {
	grantOn := getGrantOnObject(n.Targets, sqltelemetry.IncIAMGrantPrivilegesCounter)

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	grantees, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeValidation, n.Grantees,
	)
	if err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		isGrant:         true,
		withGrantOption: n.WithGrantOption,
		targets:         n.Targets,
		grantees:        grantees,
		desiredprivs:    n.Privileges,
		changePrivilege: func(
			privDesc *catpb.PrivilegeDescriptor, privileges privilege.List, grantee username.SQLUsername,
		) (changed bool) {
			// Grant the desired privileges to grantee, and return true
			// if privileges have actually been changed due to this `GRANT``.
			granteePrivsBeforeGrant := *(privDesc.FindOrCreateUser(grantee))
			privDesc.Grant(grantee, privileges, n.WithGrantOption)
			granteePrivsAfterGrant := *(privDesc.FindOrCreateUser(grantee))
			return granteePrivsBeforeGrant != granteePrivsAfterGrant
		},
		grantOn:          grantOn,
		granteesNameList: n.Grantees,
	}, nil
}

// Revoke removes privileges from users.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// Privileges: GRANT on database/table/view.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(ctx context.Context, n *tree.Revoke) (planNode, error) {
	grantOn := getGrantOnObject(n.Targets, sqltelemetry.IncIAMRevokePrivilegesCounter)

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	grantees, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeValidation, n.Grantees,
	)
	if err != nil {
		return nil, err
	}
	return &changePrivilegesNode{
		isGrant:         false,
		withGrantOption: n.GrantOptionFor,
		targets:         n.Targets,
		grantees:        grantees,
		desiredprivs:    n.Privileges,
		changePrivilege: func(
			privDesc *catpb.PrivilegeDescriptor, privileges privilege.List, grantee username.SQLUsername,
		) (changed bool) {
			granteePrivs, ok := privDesc.FindUser(grantee)
			if !ok {
				return false
			}
			granteePrivsBeforeGrant := *granteePrivs // Make a copy of the grantee's privileges before revoke.
			privDesc.Revoke(grantee, privileges, grantOn, n.GrantOptionFor)
			granteePrivs, ok = privDesc.FindUser(grantee)
			// Revoke results in any privilege changes if
			//   1. grantee's entry is removed from the privilege descriptor, or
			//   2. grantee's entry is changed in its content.
			privsChanges := !ok || granteePrivsBeforeGrant != *granteePrivs
			return privsChanges
		},
		grantOn:          grantOn,
		granteesNameList: n.Grantees,
	}, nil
}

type changePrivilegesNode struct {
	isGrant         bool
	withGrantOption bool
	targets         tree.TargetList
	grantees        []username.SQLUsername
	desiredprivs    privilege.List
	changePrivilege func(*catpb.PrivilegeDescriptor, privilege.List, username.SQLUsername) (changed bool)
	grantOn         privilege.ObjectType

	// granteesNameList is used for creating an AST node for alter default
	// privileges inside changePrivilegesNode's startExec.
	// This is required for getting the pre-normalized name to construct the AST.
	granteesNameList tree.RoleSpecList
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changePrivilegesNode) ReadingOwnWrites() {}

func (n *changePrivilegesNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	if n.withGrantOption && !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.ValidateGrantOption) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"version %v must be finalized to use grant options",
			clusterversion.ByKey(clusterversion.ValidateGrantOption))
	}

	if err := p.validateRoles(ctx, n.grantees, true /* isPublicValid */); err != nil {
		return err
	}
	// The public role is not allowed to have grant options.
	if n.isGrant && n.withGrantOption {
		for _, grantee := range n.grantees {
			if grantee.IsPublicRole() {
				return pgerror.Newf(
					pgcode.InvalidGrantOperation,
					"grant options cannot be granted to %q role",
					username.PublicRoleName(),
				)
			}
		}
	}

	var err error
	var descriptors []catalog.Descriptor
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptors, err = getDescriptorsFromTargetListForPrivilegeChange(ctx, p, n.targets)
	})
	if err != nil {
		return err
	}

	if len(descriptors) == 0 {
		return nil
	}

	var events []eventLogEntry

	// First, update the descriptors. We want to catch all errors before
	// we update them in KV below.
	b := p.txn.NewBatch()
	for _, descriptor := range descriptors {
		// Disallow privilege changes on system objects. For more context, see #43842.
		op := "REVOKE"
		if n.isGrant {
			op = "GRANT"
		}
		if catalog.IsSystemDescriptor(descriptor) {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "cannot %s on system object", op)
		}

		// The check for GRANT is only needed before the v22.1 upgrade is finalized.
		// Otherwise, we check grant options later in this function.
		if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.ValidateGrantOption) {
			if err := p.CheckPrivilege(ctx, descriptor, privilege.GRANT); err != nil {
				return err
			}
		}

		// descPrivsChanged is true if any privileges are changed on `descriptor` as a result of
		// the `GRANT` or `REVOKE` query. This allows us to no-op the `GRANT` or `REVOKE` if
		// it does not actually result in any privilege change.
		descPrivsChanged := false

		if len(n.desiredprivs) > 0 {
			grantPresent, allPresent := false, false
			var sequencePrivilegesNoOp privilege.List
			for _, priv := range n.desiredprivs {
				// Only allow granting/revoking privileges that the requesting
				// user themselves have on the descriptor.
				if err := p.CheckPrivilege(ctx, descriptor, priv); err != nil {
					return err
				}
				grantPresent = grantPresent || priv == privilege.GRANT
				allPresent = allPresent || priv == privilege.ALL

				if n.grantOn == privilege.Sequence {
					switch priv {
					case privilege.ALL,
						privilege.USAGE,
						privilege.UPDATE,
						privilege.SELECT,
						privilege.DROP,
						privilege.GRANT:
					default:
						sequencePrivilegesNoOp = append(sequencePrivilegesNoOp, priv)
					}
				}
			}
			privileges := descriptor.GetPrivileges()

			noticeMessage := ""
			if p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.ValidateGrantOption) {
				err := p.CheckGrantOptionsForUser(ctx, descriptor, n.desiredprivs, p.User(), n.isGrant)
				if err != nil {
					return err
				}

				// We only output the message for ALL privilege if it is being granted
				// without the WITH GRANT OPTION flag if GRANT privilege is involved, we
				// must always output the message
				if allPresent && n.isGrant && !n.withGrantOption {
					noticeMessage = "grant options were automatically applied but this behavior is deprecated"
				} else if grantPresent {
					noticeMessage = "the GRANT privilege is deprecated"
				}
			}

			for _, grantee := range n.grantees {
				changed := n.changePrivilege(privileges, n.desiredprivs, grantee)
				descPrivsChanged = descPrivsChanged || changed

				// TODO (sql-exp): remove the rest of this loop in 22.2.
				granteeHasGrantPriv := privileges.CheckPrivilege(grantee, privilege.GRANT)

				if granteeHasGrantPriv && n.isGrant && !n.withGrantOption && len(noticeMessage) == 0 {
					noticeMessage = "grant options were automatically applied but this behavior is deprecated"
				}
				if !n.withGrantOption && (grantPresent || allPresent || (granteeHasGrantPriv && n.isGrant)) {
					if n.isGrant {
						changed = privileges.GrantPrivilegeToGrantOptions(grantee, true /*isGrant*/)
					} else {
						changed = privileges.GrantPrivilegeToGrantOptions(grantee, false /*isGrant*/)
					}
					descPrivsChanged = descPrivsChanged || changed
				}
			}

			if len(noticeMessage) > 0 {
				params.p.BufferClientNotice(
					ctx,
					errors.WithHint(
						pgnotice.Newf("%s", noticeMessage),
						"please use WITH GRANT OPTION",
					),
				)
			}

			if len(sequencePrivilegesNoOp) > 0 {
				params.p.BufferClientNotice(
					ctx,
					pgnotice.Newf(
						"some privileges have no effect on sequences: %s",
						sequencePrivilegesNoOp.SortedNames(),
					),
				)
			}

			// Ensure superusers have exactly the allowed privilege set.
			// Postgres does not actually enforce this, instead of checking that
			// superusers have all the privileges, Postgres allows superusers to
			// bypass privilege checks.
			err = catprivilege.ValidateSuperuserPrivileges(*privileges, descriptor, n.grantOn)
			if err != nil {
				return err
			}

			// Validate privilege descriptors directly as the db/table level Validate
			// may fix up the descriptor.
			err = catprivilege.Validate(*privileges, descriptor, n.grantOn)
			if err != nil {
				return err
			}
		}

		if !descPrivsChanged {
			// no privileges will be changed from this 'GRANT' or 'REVOKE', skip it.
			continue
		}

		eventDetails := eventpb.CommonSQLPrivilegeEventDetails{}
		if n.isGrant {
			eventDetails.GrantedPrivileges = n.desiredprivs.SortedNames()
		} else {
			eventDetails.RevokedPrivileges = n.desiredprivs.SortedNames()
		}

		switch d := descriptor.(type) {
		case *dbdesc.Mutable:
			if err := p.writeDatabaseChangeToBatch(ctx, d, b); err != nil {
				return err
			}
			if err := p.createNonDropDatabaseChangeJob(ctx, d.ID,
				fmt.Sprintf("updating privileges for database %d", d.ID)); err != nil {
				return err
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, eventLogEntry{
					targetID: int32(d.ID),
					event: &eventpb.ChangeDatabasePrivilege{
						CommonSQLPrivilegeEventDetails: privs,
						DatabaseName:                   (*tree.Name)(&d.Name).String(),
					}})
			}

		case *tabledesc.Mutable:
			// TODO (lucy): This should probably have a single consolidated job like
			// DROP DATABASE.
			if err := p.createOrUpdateSchemaChangeJob(
				ctx, d,
				fmt.Sprintf("updating privileges for table %d", d.ID),
				descpb.InvalidMutationID,
			); err != nil {
				return err
			}
			if !d.Dropped() {
				if err := p.writeSchemaChangeToBatch(ctx, d, b); err != nil {
					return err
				}
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, eventLogEntry{
					targetID: int32(d.ID),
					event: &eventpb.ChangeTablePrivilege{
						CommonSQLPrivilegeEventDetails: privs,
						TableName:                      d.Name, // FIXME
					}})
			}
		case *typedesc.Mutable:
			err := p.writeTypeSchemaChange(ctx, d, fmt.Sprintf("updating privileges for type %d", d.ID))
			if err != nil {
				return err
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, eventLogEntry{
					targetID: int32(d.ID),
					event: &eventpb.ChangeTypePrivilege{
						CommonSQLPrivilegeEventDetails: privs,
						TypeName:                       d.Name, // FIXME
					}})
			}
		case *schemadesc.Mutable:
			if err := p.writeSchemaDescChange(
				ctx,
				d,
				fmt.Sprintf("updating privileges for schema %d", d.ID),
			); err != nil {
				return err
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, eventLogEntry{
					targetID: int32(d.ID),
					event: &eventpb.ChangeSchemaPrivilege{
						CommonSQLPrivilegeEventDetails: privs,
						SchemaName:                     d.Name, // FIXME
					}})
			}
		}
	}

	// Now update the descriptors transactionally.
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Record the privilege changes in the event log. This is an
	// auditable log event and is recorded in the same transaction as
	// the table descriptor update.
	if events != nil {
		if err := params.p.logEvents(params.ctx, events...); err != nil {
			return err
		}
	}
	return nil
}

func (*changePrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changePrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changePrivilegesNode) Close(context.Context)        {}

// getGrantOnObject returns the type of object being granted on based on the TargetList.
// getGrantOnObject also calls incIAMFunc with the object type name.
func getGrantOnObject(targets tree.TargetList, incIAMFunc func(on string)) privilege.ObjectType {
	switch {
	case targets.Databases != nil:
		incIAMFunc(sqltelemetry.OnDatabase)
		return privilege.Database
	case targets.AllSequencesInSchema:
		incIAMFunc(sqltelemetry.OnAllSequencesInSchema)
		return privilege.Sequence
	case targets.AllTablesInSchema:
		incIAMFunc(sqltelemetry.OnAllTablesInSchema)
		return privilege.Table
	case targets.Schemas != nil:
		incIAMFunc(sqltelemetry.OnSchema)
		return privilege.Schema
	case targets.Types != nil:
		incIAMFunc(sqltelemetry.OnType)
		return privilege.Type
	default:
		if targets.Tables.IsSequence {
			incIAMFunc(sqltelemetry.OnSequence)
			return privilege.Sequence
		}
		incIAMFunc(sqltelemetry.OnTable)
		return privilege.Table
	}
}

// validateRoles checks that all the roles are valid users.
// isPublicValid determines whether or not Public is a valid role.
func (p *planner) validateRoles(
	ctx context.Context, roles []username.SQLUsername, isPublicValid bool,
) error {
	users, err := p.GetAllRoles(ctx)
	if err != nil {
		return err
	}
	if isPublicValid {
		users[username.PublicRoleName()] = true // isRole
	}
	for i, grantee := range roles {
		if _, ok := users[grantee]; !ok {
			sqlName := tree.Name(roles[i].Normalized())
			return errors.Errorf("user or role %s does not exist", &sqlName)
		}
	}

	return nil
}
