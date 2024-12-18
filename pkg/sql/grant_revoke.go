// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

// Grant adds privileges to users.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// Privileges: GRANT on database/table/view.
//
//	Notes: postgres requires the object owner.
//	       mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Grant(ctx context.Context, n *tree.Grant) (planNode, error) {
	grantOn, err := p.getGrantOnObject(ctx, n.Targets, sqltelemetry.IncIAMGrantPrivilegesCounter)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get the privileges on the grant targets")
	}
	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	grantees, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeValidation, n.Grantees,
	)
	if err != nil {
		return nil, err
	}

	if !grantOn.IsDescriptorBacked() {
		return &changeNonDescriptorBackedPrivilegesNode{
			changePrivilegesNode: changePrivilegesNode{
				isGrant:         true,
				withGrantOption: n.WithGrantOption,
				targets:         n.Targets,
				grantees:        grantees,
				desiredprivs:    n.Privileges,
				grantOn:         grantOn,
			},
		}, nil
	}

	return &changeDescriptorBackedPrivilegesNode{
		changePrivilegesNode: changePrivilegesNode{
			isGrant:         true,
			withGrantOption: n.WithGrantOption,
			targets:         n.Targets,
			grantees:        grantees,
			desiredprivs:    n.Privileges,
			grantOn:         grantOn,
		},
		changePrivilege: func(
			privDesc *catpb.PrivilegeDescriptor, privileges privilege.List, grantee username.SQLUsername,
		) (changed bool, retErr error) {
			// Grant the desired privileges to grantee, and return true
			// if privileges have actually been changed due to this `GRANT``.
			granteePrivsBeforeGrant := *(privDesc.FindOrCreateUser(grantee))
			privDesc.Grant(grantee, privileges, n.WithGrantOption)
			granteePrivsAfterGrant := *(privDesc.FindOrCreateUser(grantee))
			return granteePrivsBeforeGrant != granteePrivsAfterGrant, nil
		},
	}, nil
}

// Revoke removes privileges from users.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// Privileges: GRANT on database/table/view.
//
//	Notes: postgres requires the object owner.
//	       mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(ctx context.Context, n *tree.Revoke) (planNode, error) {
	grantOn, err := p.getGrantOnObject(ctx, n.Targets, sqltelemetry.IncIAMRevokePrivilegesCounter)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get the privileges on the grant targets")
	}

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	grantees, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeValidation, n.Grantees,
	)
	if err != nil {
		return nil, err
	}

	if !grantOn.IsDescriptorBacked() {
		return &changeNonDescriptorBackedPrivilegesNode{
			changePrivilegesNode: changePrivilegesNode{
				isGrant:         false,
				withGrantOption: n.GrantOptionFor,
				targets:         n.Targets,
				grantees:        grantees,
				desiredprivs:    n.Privileges,
				grantOn:         grantOn,
			},
		}, nil
	}

	return &changeDescriptorBackedPrivilegesNode{
		changePrivilegesNode: changePrivilegesNode{
			isGrant:         false,
			withGrantOption: n.GrantOptionFor,
			targets:         n.Targets,
			grantees:        grantees,
			desiredprivs:    n.Privileges,
			grantOn:         grantOn,
		},
		changePrivilege: func(
			privDesc *catpb.PrivilegeDescriptor, privileges privilege.List, grantee username.SQLUsername,
		) (changed bool, retErr error) {
			granteePrivs, ok := privDesc.FindUser(grantee)
			if !ok {
				return false, nil
			}
			granteePrivsBeforeGrant := *granteePrivs // Make a copy of the grantee's privileges before revoke.
			if err := privDesc.Revoke(grantee, privileges, grantOn, n.GrantOptionFor); err != nil {
				return false, err
			}
			granteePrivs, ok = privDesc.FindUser(grantee)
			// Revoke results in any privilege changes if
			//   1. grantee's entry is removed from the privilege descriptor, or
			//   2. grantee's entry is changed in its content.
			privsChanges := !ok || granteePrivsBeforeGrant != *granteePrivs
			return privsChanges, nil
		},
	}, nil
}

type changePrivilegesNode struct {
	isGrant         bool
	withGrantOption bool
	grantees        []username.SQLUsername
	desiredprivs    privilege.List
	targets         tree.GrantTargetList
	grantOn         privilege.ObjectType
}

type changeDescriptorBackedPrivilegesNode struct {
	zeroInputPlanNode
	changePrivilegesNode
	changePrivilege func(*catpb.PrivilegeDescriptor, privilege.List, username.SQLUsername) (changed bool, retErr error)
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changeDescriptorBackedPrivilegesNode) ReadingOwnWrites() {}

func (p *planner) preChangePrivilegesValidation(
	ctx context.Context, grantees []username.SQLUsername, withGrantOption, isGrant bool,
) error {
	if err := p.validateRoles(ctx, grantees, true /* isPublicValid */); err != nil {
		return err
	}
	// The public role is not allowed to have grant options.
	if isGrant && withGrantOption {
		for _, grantee := range grantees {
			if grantee.IsPublicRole() {
				return pgerror.Newf(
					pgcode.InvalidGrantOperation,
					"grant options cannot be granted to %q role",
					username.PublicRoleName(),
				)
			}
		}
	}
	return nil
}

func (n *changeDescriptorBackedPrivilegesNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	if err := params.p.preChangePrivilegesValidation(params.ctx, n.grantees, n.withGrantOption, n.isGrant); err != nil {
		return err
	}

	var err error
	var descriptorsWithTypes []DescriptorWithObjectType
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptorsWithTypes, err = p.getDescriptorsFromTargetListForPrivilegeChange(ctx, n.targets)
	})
	if err != nil {
		return err
	}

	if len(descriptorsWithTypes) == 0 {
		return nil
	}

	var events []logpb.EventPayload

	// First, update the descriptors. We want to catch all errors before
	// we update them in KV below.
	b := p.txn.NewBatch()
	for _, descriptorWithType := range descriptorsWithTypes {
		// Disallow privilege changes on system objects. For more context, see #43842.
		descriptor := descriptorWithType.descriptor
		objType := descriptorWithType.objectType

		if catalog.IsSystemDescriptor(descriptor) {

			op := "REVOKE"
			if n.isGrant {
				op = "GRANT"
			}
			return pgerror.Newf(pgcode.InsufficientPrivilege, "cannot %s on system object", op)
		}

		// descPrivsChanged is true if any privileges are changed on `descriptor` as a result of
		// the `GRANT` or `REVOKE` query. This allows us to no-op the `GRANT` or `REVOKE` if
		// it does not actually result in any privilege change.
		descPrivsChanged := false

		if len(n.desiredprivs) > 0 {
			privsToIgnore := privilege.List{}
			unsupportedPrivsForType := make(map[privilege.ObjectType]struct{})
			for _, priv := range n.desiredprivs {
				// Only allow granting/revoking privileges that the requesting
				// user themselves have on the descriptor.
				if err := p.CheckPrivilege(ctx, descriptor, priv); err != nil {
					return err
				}
				// Track privileges that do not apply to sequences.
				if objType == privilege.Sequence {
					switch priv {
					case privilege.ALL,
						privilege.USAGE,
						privilege.UPDATE,
						privilege.SELECT,
						privilege.DROP:
					default:
						privsToIgnore = append(privsToIgnore, priv)
					}
				}
			}

			err := p.MustCheckGrantOptionsForUser(ctx, descriptor.GetPrivileges(), descriptor, n.desiredprivs, p.User(), n.isGrant)
			if err != nil {
				return err
			}

			privileges := descriptor.GetPrivileges()
			// Ensure we are only setting privilleges valid for this object type.
			// i.e. We only expect this for sequences.
			validPrivs, err := privilege.GetValidPrivilegesForObject(objType)
			if err != nil {
				return err
			}
			targetPrivs := n.desiredprivs.ToBitField() & validPrivs.ToBitField()
			// If there are privs that are ignored for sequences, lets exclude
			// those here.
			privsToIgnoreBits := privsToIgnore.ToBitField()
			targetPrivs = targetPrivs & (^privsToIgnoreBits)
			// If any privileges have been dropped or ignored, then lets log
			// a message for this type.
			if targetPrivs != n.desiredprivs.ToBitField() {
				missingPrivs, err := privilege.ListFromBitField(
					n.desiredprivs.ToBitField()&(^targetPrivs), privilege.Any)
				if err != nil {
					return err
				}
				if _, ok := unsupportedPrivsForType[objType]; !ok {
					params.p.BufferClientNotice(
						ctx,
						pgnotice.Newf(
							"some privileges have no effect on %ss: %s",
							objType,
							missingPrivs.SortedDisplayNames(),
						),
					)
					unsupportedPrivsForType[objType] = struct{}{}
				}
				// For the purpose of revoke restore ignored fields back,
				// so we can at least remove them from the object.
				if !n.isGrant {
					targetPrivs = targetPrivs | privsToIgnoreBits
				}
				// If nothing will be applied move to the next descriptor.
				if targetPrivs == 0 {
					continue
				}
			}
			privsToSet, err := privilege.ListFromBitField(targetPrivs, objType)
			if err != nil {
				return err
			}
			for _, grantee := range n.grantees {
				changed, err := n.changePrivilege(privileges, privsToSet, grantee)
				if err != nil {
					return err
				}
				descPrivsChanged = descPrivsChanged || changed
				if !n.isGrant && grantee == privileges.Owner() {
					params.p.BufferClientNotice(
						ctx,
						pgnotice.Newf(
							"%s is the owner of %s and still has all privileges implicitly",
							privileges.Owner(),
							descriptor.GetName(),
						),
					)
				}
			}
			// Ensure superusers have exactly the allowed privilege set.
			// Postgres does not actually enforce this, instead of checking that
			// superusers have all the privileges, Postgres allows superusers to
			// bypass privilege checks.
			err = catprivilege.ValidateSuperuserPrivileges(*privileges, descriptor, objType)
			if err != nil {
				return err
			}

			// Validate privilege descriptors directly as the db/table level Validate
			// may fix up the descriptor.
			err = catprivilege.Validate(*privileges, descriptor, objType)
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
			eventDetails.GrantedPrivileges = n.desiredprivs.SortedDisplayNames()
		} else {
			eventDetails.RevokedPrivileges = n.desiredprivs.SortedDisplayNames()
		}

		switch d := descriptor.(type) {
		case *dbdesc.Mutable:
			if err := p.writeDatabaseChangeToBatch(ctx, d, b); err != nil {
				return err
			}
			if err := p.createNonDropDatabaseChangeJob(ctx, d.ID, fmt.Sprintf("updating privileges for database %d", d.ID)); err != nil {
				return err
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, &eventpb.ChangeDatabasePrivilege{
					CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
						DescriptorID: uint32(d.ID),
					},
					CommonSQLPrivilegeEventDetails: privs,
					DatabaseName:                   (*tree.Name)(&d.Name).String(),
				})
			}
		case *tabledesc.Mutable:
			if !d.Dropped() {
				if err := p.writeSchemaChangeToBatch(ctx, d, b); err != nil {
					return err
				}
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, &eventpb.ChangeTablePrivilege{
					CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
						DescriptorID: uint32(d.ID),
					},
					CommonSQLPrivilegeEventDetails: privs,
					TableName:                      d.Name, // FIXME
				})
			}
		case *typedesc.Mutable:
			if !d.Dropped() {
				err := p.writeDescToBatch(ctx, d, b)
				if err != nil {
					return err
				}
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, &eventpb.ChangeTypePrivilege{
					CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
						DescriptorID: uint32(d.ID),
					},
					CommonSQLPrivilegeEventDetails: privs,
					TypeName:                       d.Name, // FIXME
				})
			}
		case *schemadesc.Mutable:
			if !d.Dropped() {
				err := p.writeDescToBatch(ctx, d, b)
				if err != nil {
					return err
				}
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, &eventpb.ChangeSchemaPrivilege{
					CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
						DescriptorID: uint32(d.ID),
					},
					CommonSQLPrivilegeEventDetails: privs,
					SchemaName:                     d.Name, // FIXME
				})
			}
		case *funcdesc.Mutable:
			if !d.Dropped() {
				err := p.writeDescToBatch(ctx, d, b)
				if err != nil {
					return err
				}
			}
			for _, grantee := range n.grantees {
				privs := eventDetails // copy the granted/revoked privilege list.
				privs.Grantee = grantee.Normalized()
				events = append(events, &eventpb.ChangeFunctionPrivilege{
					CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
						DescriptorID: uint32(d.ID),
					},
					CommonSQLPrivilegeEventDetails: privs,
					FuncName:                       d.Name, // FIXME
				})
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

func (*changeDescriptorBackedPrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changeDescriptorBackedPrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changeDescriptorBackedPrivilegesNode) Close(context.Context)        {}

// getGrantOnObject returns the type of object being granted on based on the
// TargetList.
// getGrantOnObject also calls incIAMFunc with the object type name.
// Note that the "GRANT ... ON obj_names" syntax supports both sequence name
// and table name in the "obj_names" field.
// If the target list contains a table, this function always returns
// privilege.Table. Only when all objects in the target list are sequence, it
// returns the privilege.Sequence.
func (p *planner) getGrantOnObject(
	ctx context.Context, targets tree.GrantTargetList, incIAMFunc func(on string),
) (privilege.ObjectType, error) {
	switch {
	case targets.Databases != nil:
		incIAMFunc(sqltelemetry.OnDatabase)
		return privilege.Database, nil
	case targets.AllSequencesInSchema:
		incIAMFunc(sqltelemetry.OnAllSequencesInSchema)
		return privilege.Sequence, nil
	case targets.AllTablesInSchema:
		incIAMFunc(sqltelemetry.OnAllTablesInSchema)
		return privilege.Table, nil
	case targets.AllFunctionsInSchema:
		incIAMFunc(sqltelemetry.OnAllFunctionsInSchema)
		return privilege.Routine, nil
	case targets.AllProceduresInSchema:
		incIAMFunc(sqltelemetry.OnAllProceduresInSchema)
		return privilege.Routine, nil
	case targets.Schemas != nil:
		incIAMFunc(sqltelemetry.OnSchema)
		return privilege.Schema, nil
	case targets.Types != nil:
		incIAMFunc(sqltelemetry.OnType)
		return privilege.Type, nil
	case targets.Functions != nil:
		incIAMFunc(sqltelemetry.OnFunction)
		return privilege.Routine, nil
	case targets.Procedures != nil:
		incIAMFunc(sqltelemetry.OnProcedure)
		return privilege.Routine, nil
	case targets.System:
		incIAMFunc(sqltelemetry.OnSystem)
		return privilege.Global, nil
	case targets.ExternalConnections != nil:
		incIAMFunc(sqltelemetry.OnExternalConnection)
		return privilege.ExternalConnection, nil
	default:
		composition, err := p.getTablePatternsComposition(ctx, targets)
		if err != nil {
			return privilege.Any, errors.Wrap(
				err,
				"cannot determine the target type of the GRANT statement",
			)
		}
		if composition == containsTable {
			incIAMFunc(sqltelemetry.OnTable)
			return privilege.Table, nil
		}
		if composition == virtualTablesOnly {
			return privilege.VirtualTable, nil
		}
		incIAMFunc(sqltelemetry.OnSequence)
		return privilege.Sequence, nil
	}
}

// tablePatternsComposition is an enum to mark the composition of the
// TablePatterns in the GRANT/REVOKE statement's target list.

type tablePatternsComposition int8

const (
	unknownComposition tablePatternsComposition = iota
	// If all targets are sequences.
	sequenceOnly
	// If there's any table in the target list.
	containsTable
	// If all targets are virtual tables.
	virtualTablesOnly
)

// getTablePatternsComposition gets the given grant target list's
// object type composition. This is used to determine the privilege list for
// the targets.
// If all targets are of type sequence, then we should use the sequence
// privilege list; if any target is of type table, we should use the table
// privilege.
// This is because the table privilege is the subset of sequence privilege.
func (p *planner) getTablePatternsComposition(
	ctx context.Context, targets tree.GrantTargetList,
) (tablePatternsComposition, error) {
	if targets.Tables.SequenceOnly {
		return sequenceOnly, nil
	}
	var allObjectIDs []descpb.ID
	for _, tableTarget := range targets.Tables.TablePatterns {
		tableGlob, err := tableTarget.NormalizeTablePattern()
		if err != nil {
			return unknownComposition, err
		}
		_, objectIDs, err := p.ExpandTableGlob(ctx, tableGlob)
		if err != nil {
			return unknownComposition, err
		}
		allObjectIDs = append(allObjectIDs, objectIDs...)
	}

	if len(allObjectIDs) == 0 {
		return unknownComposition, nil
	}

	// Check if the table is a virtual table.
	var virtualIDs descpb.IDs
	var nonVirtualIDs descpb.IDs
	for _, objectID := range allObjectIDs {
		isVirtual := false
		for _, vs := range virtualSchemas {
			if _, ok := vs.tableDefs[objectID]; ok {
				isVirtual = true
				break
			}
		}

		if isVirtual {
			virtualIDs = append(nonVirtualIDs, objectID)
		} else {
			nonVirtualIDs = append(virtualIDs, objectID)
		}
	}
	haveVirtualTables := virtualIDs.Len() > 0
	haveNonVirtualTables := nonVirtualIDs.Len() > 0
	if haveVirtualTables && haveNonVirtualTables {
		return unknownComposition, pgerror.Newf(
			pgcode.FeatureNotSupported, "cannot mix grants between virtual and non-virtual tables",
		)
	}
	if !haveNonVirtualTables {
		return virtualTablesOnly, nil
	}
	// Note that part of the reason the code is structured this way is that
	// resolving mutable descriptors for virtual table IDs results in an error.
	muts, err := p.Descriptors().MutableByID(p.txn).Descs(ctx, nonVirtualIDs)
	if err != nil {
		return unknownComposition, err
	}

	for _, mut := range muts {
		if mut != nil && mut.DescriptorType() == catalog.Table {
			tableDesc, err := catalog.AsTableDescriptor(mut)
			if err != nil {
				return unknownComposition, err
			}
			if !tableDesc.IsSequence() {
				return containsTable, nil
			}
		}
	}
	return sequenceOnly, nil
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
			return sqlerrors.NewUndefinedUserError(roles[i])
		}
	}

	return nil
}
