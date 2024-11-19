// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbdesc

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// DatabaseDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for database descriptors.
type DatabaseDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableDatabase() catalog.DatabaseDescriptor
	BuildExistingMutableDatabase() *Mutable
	BuildCreatedMutableDatabase() *Mutable
}

type databaseDescriptorBuilder struct {
	original             *descpb.DatabaseDescriptor
	maybeModified        *descpb.DatabaseDescriptor
	mvccTimestamp        hlc.Timestamp
	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
	// This is the raw bytes (tag + data) of the database descriptor in storage.
	rawBytesInStorage []byte
}

var _ DatabaseDescriptorBuilder = &databaseDescriptorBuilder{}

// NewBuilder returns a new DatabaseDescriptorBuilder instance by delegating to
// NewBuilderWithMVCCTimestamp with an empty MVCC timestamp.
//
// Callers must assume that the given protobuf has already been treated with the
// MVCC timestamp beforehand.
func NewBuilder(desc *descpb.DatabaseDescriptor) DatabaseDescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// NewBuilderWithMVCCTimestamp creates a new DatabaseDescriptorBuilder instance
// for building table descriptors.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.DatabaseDescriptor, mvccTimestamp hlc.Timestamp,
) DatabaseDescriptorBuilder {
	return newBuilder(
		desc,
		mvccTimestamp,
		false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{},
	)
}

func newBuilder(
	desc *descpb.DatabaseDescriptor,
	mvccTimestamp hlc.Timestamp,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) DatabaseDescriptorBuilder {
	return &databaseDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.DatabaseDescriptor),
		mvccTimestamp:        mvccTimestamp,
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (ddb *databaseDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Database
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (ddb *databaseDescriptorBuilder) RunPostDeserializationChanges() (err error) {
	defer func() {
		err = errors.Wrapf(err, "database %q (%d)", ddb.original.Name, ddb.original.ID)
	}()
	// Set the ModificationTime field before doing anything else.
	// Other changes may depend on it.
	mustSetModTime, err := descpb.MustSetModificationTime(
		ddb.original.ModificationTime, ddb.mvccTimestamp, ddb.original.Version, ddb.original.State,
	)
	if err != nil {
		return err
	}
	ddb.maybeModified = protoutil.Clone(ddb.original).(*descpb.DatabaseDescriptor)
	if mustSetModTime {
		ddb.maybeModified.ModificationTime = ddb.mvccTimestamp
		ddb.changes.Add(catalog.SetModTimeToMVCCTimestamp)
	}

	// This should only every happen to the system database. Unlike many other
	// post-deserialization changes, this does not need to last forever to
	// support restores. It can be removed after a migration performing such a
	// migration occurs.
	//
	// TODO(ajwerner): Write or piggy-back off some other migration to rewrite
	// the descriptor, and then remove this in a release that is no longer
	// compatible with the predecessor of the version with the migration.
	if ddb.maybeModified.Version == 0 {
		ddb.maybeModified.Version = 1
		ddb.changes.Add(catalog.SetSystemDatabaseDescriptorVersion)
	}

	createdDefaultPrivileges := false
	removedIncompatibleDatabasePrivs := false
	// Skip converting incompatible privileges to default privileges on the
	// system database and let MaybeFixPrivileges handle it instead as we do not
	// want any default privileges on the system database.
	if ddb.original.GetID() != keys.SystemDatabaseID {
		if ddb.maybeModified.DefaultPrivileges == nil {
			ddb.maybeModified.DefaultPrivileges = catprivilege.MakeDefaultPrivilegeDescriptor(
				catpb.DefaultPrivilegeDescriptor_DATABASE)
			createdDefaultPrivileges = true
		}

		removedIncompatibleDatabasePrivs = maybeConvertIncompatibleDBPrivilegesToDefaultPrivileges(
			ddb.maybeModified.Privileges, ddb.maybeModified.DefaultPrivileges,
		)
	}

	privsChanged, err := catprivilege.MaybeFixPrivileges(
		&ddb.maybeModified.Privileges,
		descpb.InvalidID,
		descpb.InvalidID,
		privilege.Database,
		ddb.maybeModified.GetName())
	if err != nil {
		return err
	}
	if privsChanged || removedIncompatibleDatabasePrivs || createdDefaultPrivileges {
		ddb.changes.Add(catalog.UpgradedPrivileges)
	}
	if maybeRemoveDroppedSelfEntryFromSchemas(ddb.maybeModified) {
		ddb.changes.Add(catalog.RemovedSelfEntryInSchemas)
	}
	return nil
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (ddb *databaseDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) error {
	// Upgrade the declarative schema changer state.
	if scpb.MigrateDescriptorState(version, descpb.InvalidID, ddb.maybeModified.DeclarativeSchemaChangerState) {
		ddb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}
	return nil
}

// StripDanglingBackReferences implements the catalog.DescriptorBuilder
// interface.
func (ddb *databaseDescriptorBuilder) StripDanglingBackReferences(
	descIDMightExist func(id descpb.ID) bool, nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
) error {
	for schemaName, schemaInfo := range ddb.maybeModified.Schemas {
		if !descIDMightExist(schemaInfo.ID) {
			delete(ddb.maybeModified.Schemas, schemaName)
			ddb.changes.Add(catalog.StrippedDanglingBackReferences)
		}
	}
	return nil
}

// StripNonExistentRoles implements the catalog.DescriptorBuilder
// interface.
func (ddb *databaseDescriptorBuilder) StripNonExistentRoles(
	roleExists func(role username.SQLUsername) bool,
) error {
	// Remove any non-existent roles from default privileges.
	defaultPrivs := ddb.original.GetDefaultPrivileges()
	if defaultPrivs != nil {
		err := ddb.stripNonExistentRolesOnDefaultPrivs(ddb.original.DefaultPrivileges.DefaultPrivilegesPerRole, roleExists)
		if err != nil {
			return err
		}
	}

	// If the owner doesn't exist, change the owner to admin.
	if !roleExists(ddb.maybeModified.GetPrivileges().Owner()) {
		ddb.maybeModified.Privileges.OwnerProto = username.AdminRoleName().EncodeProto()
		ddb.changes.Add(catalog.StrippedNonExistentRoles)
	}

	// Remove any non-existent roles from the privileges.
	newPrivs := make([]catpb.UserPrivileges, 0, len(ddb.maybeModified.Privileges.Users))
	for _, priv := range ddb.maybeModified.Privileges.Users {
		exists := roleExists(priv.UserProto.Decode())
		if exists {
			newPrivs = append(newPrivs, priv)
		}
	}
	if len(newPrivs) != len(ddb.maybeModified.Privileges.Users) {
		ddb.maybeModified.Privileges.Users = newPrivs
		ddb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	return nil
}

func (ddb *databaseDescriptorBuilder) stripNonExistentRolesOnDefaultPrivs(
	defaultPrivs []catpb.DefaultPrivilegesForRole, roleExists func(role username.SQLUsername) bool,
) error {
	hasChanges := false
	newDefaultPrivs := make([]catpb.DefaultPrivilegesForRole, 0, len(defaultPrivs))
	for _, dp := range defaultPrivs {
		// Skip adding if we are dealing with an explicit role and the role does
		// not exist.
		if dp.IsExplicitRole() && !roleExists(dp.GetExplicitRole().UserProto.Decode()) {
			hasChanges = true
			continue
		}

		newDefaultPrivilegesPerObject := make(map[privilege.TargetObjectType]catpb.PrivilegeDescriptor, len(dp.DefaultPrivilegesPerObject))

		for objTyp, privDesc := range dp.DefaultPrivilegesPerObject {
			newUserPrivs := make([]catpb.UserPrivileges, 0, len(privDesc.Users))
			for _, userPriv := range privDesc.Users {
				// Only add users where the role exists.
				if roleExists(userPriv.UserProto.Decode()) {
					newUserPrivs = append(newUserPrivs, userPriv)
				} else {
					hasChanges = true
				}
			}
			// If we have not filtered out all user privileges, update the privilege
			// descriptor with our newUserPrivs -- along with our map.
			if len(newUserPrivs) != 0 {
				privDesc.Users = newUserPrivs
				newDefaultPrivilegesPerObject[objTyp] = privDesc
			}
		}

		// If we have not filtered out our map of default privileges, update the
		// default privileges for role and add that to our list of newDefaultPrivs.
		if len(newDefaultPrivilegesPerObject) != 0 {
			dp.DefaultPrivilegesPerObject = newDefaultPrivilegesPerObject
			newDefaultPrivs = append(newDefaultPrivs, dp)
		}
	}

	if hasChanges {
		ddb.maybeModified.DefaultPrivileges.DefaultPrivilegesPerRole = newDefaultPrivs
		ddb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	return nil
}

// SetRawBytesInStorage implements the catalog.DescriptorBuilder interface.
func (ddb *databaseDescriptorBuilder) SetRawBytesInStorage(rawBytes []byte) {
	ddb.rawBytesInStorage = append([]byte(nil), rawBytes...) // deep-copy
}

func maybeConvertIncompatibleDBPrivilegesToDefaultPrivileges(
	privileges *catpb.PrivilegeDescriptor, defaultPrivileges *catpb.DefaultPrivilegeDescriptor,
) (hasChanged bool) {
	// If privileges are nil, there is nothing to convert.
	// This case can happen during restore where privileges are not yet created.
	if privileges == nil {
		return false
	}

	var pgIncompatibleDBPrivileges = privilege.List{
		privilege.SELECT, privilege.INSERT, privilege.UPDATE, privilege.DELETE,
	}

	for i, user := range privileges.Users {
		incompatiblePrivileges := user.Privileges & pgIncompatibleDBPrivileges.ToBitField()

		if incompatiblePrivileges == 0 {
			continue
		}

		hasChanged = true

		// XOR to remove incompatible privileges.
		user.Privileges ^= incompatiblePrivileges

		privileges.Users[i] = user

		// Convert the incompatible privileges to default privileges.
		role := defaultPrivileges.FindOrCreateUser(catpb.DefaultPrivilegesRole{ForAllRoles: true})
		tableDefaultPrivilegesForAllRoles := role.DefaultPrivilegesPerObject[privilege.Tables]

		defaultPrivilegesForUser := tableDefaultPrivilegesForAllRoles.FindOrCreateUser(user.User())
		defaultPrivilegesForUser.Privileges |= incompatiblePrivileges

		role.DefaultPrivilegesPerObject[privilege.Tables] = tableDefaultPrivilegesForAllRoles
	}

	return hasChanged
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (ddb *databaseDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return ddb.BuildImmutableDatabase()
}

// BuildImmutableDatabase returns an immutable database descriptor.
func (ddb *databaseDescriptorBuilder) BuildImmutableDatabase() catalog.DatabaseDescriptor {
	desc := ddb.maybeModified
	if desc == nil {
		desc = ddb.original
	}
	return &immutable{
		DatabaseDescriptor:   *desc,
		isUncommittedVersion: ddb.isUncommittedVersion,
		changes:              ddb.changes,
		rawBytesInStorage:    append([]byte(nil), ddb.rawBytesInStorage...), // deep-copy
	}
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (ddb *databaseDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return ddb.BuildExistingMutableDatabase()
}

// BuildExistingMutableDatabase returns a mutable descriptor for a database
// which already exists.
func (ddb *databaseDescriptorBuilder) BuildExistingMutableDatabase() *Mutable {
	if ddb.maybeModified == nil {
		ddb.maybeModified = protoutil.Clone(ddb.original).(*descpb.DatabaseDescriptor)
	}
	return &Mutable{
		immutable: immutable{
			DatabaseDescriptor:   *ddb.maybeModified,
			changes:              ddb.changes,
			isUncommittedVersion: ddb.isUncommittedVersion,
			rawBytesInStorage:    append([]byte(nil), ddb.rawBytesInStorage...), // deep-copy
		},
		ClusterVersion: &immutable{DatabaseDescriptor: *ddb.original},
	}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (ddb *databaseDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return ddb.BuildCreatedMutableDatabase()
}

// BuildCreatedMutableDatabase returns a mutable descriptor for a database
// which is in the process of being created.
func (ddb *databaseDescriptorBuilder) BuildCreatedMutableDatabase() *Mutable {
	desc := ddb.maybeModified
	if desc == nil {
		desc = ddb.original
	}
	return &Mutable{
		immutable: immutable{
			DatabaseDescriptor:   *desc,
			changes:              ddb.changes,
			isUncommittedVersion: ddb.isUncommittedVersion,
			rawBytesInStorage:    append([]byte(nil), ddb.rawBytesInStorage...), // deep-copy
		},
	}
}

// NewInitialOption is an optional argument for NewInitial.
type NewInitialOption func(*descpb.DatabaseDescriptor)

// MaybeWithDatabaseRegionConfig is an option allowing an optional regional
// configuration to be set on the database descriptor.
func MaybeWithDatabaseRegionConfig(regionConfig *multiregion.RegionConfig) NewInitialOption {
	return func(desc *descpb.DatabaseDescriptor) {
		// Not a multi-region database. Not much to do here.
		if regionConfig == nil {
			return
		}
		desc.RegionConfig = &descpb.DatabaseDescriptor_RegionConfig{
			SurvivalGoal:    regionConfig.SurvivalGoal(),
			PrimaryRegion:   regionConfig.PrimaryRegion(),
			RegionEnumID:    regionConfig.RegionEnumID(),
			Placement:       regionConfig.Placement(),
			SecondaryRegion: regionConfig.SecondaryRegion(),
		}
	}
}

// WithPublicSchemaID is used to create a DatabaseDescriptor with a
// publicSchemaID.
func WithPublicSchemaID(publicSchemaID descpb.ID) NewInitialOption {
	return func(desc *descpb.DatabaseDescriptor) {
		// TODO(richardjcai): Remove this in 22.2. If the public schema id is
		// keys.PublicSchemaID, we do not add an entry as the public schema does
		// not have a descriptor.
		if publicSchemaID != keys.PublicSchemaID {
			desc.Schemas = map[string]descpb.DatabaseDescriptor_SchemaInfo{
				catconstants.PublicSchemaName: {ID: publicSchemaID},
			}
		}
	}
}

// NewInitial constructs a new Mutable for an initial version from an id and
// name with default privileges.
func NewInitial(
	id descpb.ID, name string, owner username.SQLUsername, options ...NewInitialOption,
) *Mutable {
	return newInitialWithPrivileges(
		id,
		name,
		catpb.NewBaseDatabasePrivilegeDescriptor(owner),
		catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE),
		options...,
	)
}

// newInitialWithPrivileges constructs a new Mutable for an initial version
// from an id and name and custom privileges.
func newInitialWithPrivileges(
	id descpb.ID,
	name string,
	privileges *catpb.PrivilegeDescriptor,
	defaultPrivileges *catpb.DefaultPrivilegeDescriptor,
	options ...NewInitialOption,
) *Mutable {
	ret := descpb.DatabaseDescriptor{
		Name:              name,
		ID:                id,
		Version:           1,
		Privileges:        privileges,
		DefaultPrivileges: defaultPrivileges,
	}
	for _, option := range options {
		option(&ret)
	}
	return NewBuilder(&ret).BuildCreatedMutableDatabase()
}
