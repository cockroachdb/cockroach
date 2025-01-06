// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobsauth_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

type userPrivilege struct {
	privilege.Kind
	privilege.ObjectType
}

var viewJobGlobalPrivilege = userPrivilege{privilege.VIEWJOB, privilege.Global}

var controlJobGlobalPrivilege = userPrivilege{privilege.CONTROLJOB, privilege.Global}

type testAuthAccessor struct {
	user username.SQLUsername

	// set of role options that the user has
	roleOptions map[roleoption.Option]struct{}

	// set of privileges that the user has
	userPrivileges map[userPrivilege]struct{}

	// set of descriptors which the user has privilege.CHANGEFEED on
	changeFeedPrivileges map[descpb.ID]struct{}
	// set of descriptors which are dropped
	droppedDescriptors map[descpb.ID]struct{}

	// set of all usernames who are admins
	admins map[string]struct{}

	members map[username.SQLUsername]bool

	rand *rand.Rand
}

var _ jobsauth.AuthorizationAccessor = &testAuthAccessor{}

func (a *testAuthAccessor) CheckPrivilegeForTableID(
	_ context.Context, tableID descpb.ID, _ privilege.Kind,
) error {
	if _, ok := a.droppedDescriptors[tableID]; ok {
		if a.rand.Int31n(2) == 0 {
			return catalog.ErrDescriptorDropped
		} else {
			return pgerror.New(pgcode.UndefinedTable, "foo")
		}
	}

	if _, ok := a.changeFeedPrivileges[tableID]; !ok {
		return pgerror.New(pgcode.InsufficientPrivilege, "foo")
	}
	return nil
}

// HasPrivilege is only implemented for the target user.
func (a *testAuthAccessor) HasPrivilege(
	ctx context.Context,
	privilegeObject privilege.Object,
	privilegeKind privilege.Kind,
	sqlUsername username.SQLUsername,
) (bool, error) {
	if sqlUsername != a.user {
		panic(fmt.Sprintf("testAuthAccessor does not implement HasPrivilege for user %s", sqlUsername))
	}
	_, ok := a.userPrivileges[userPrivilege{privilegeKind, privilegeObject.GetObjectType()}]
	if ok {
		return true, nil
	}
	return false, nil
}

func (a *testAuthAccessor) UserHasRoleOption(
	_ context.Context, user username.SQLUsername, roleOption roleoption.Option,
) (bool, error) {
	if user != a.user {
		panic(fmt.Sprintf("testAuthAccessor does not implement UserHasRoleOption for user %s", user))
	}
	_, ok := a.roleOptions[roleOption]
	return ok, nil
}

func (a *testAuthAccessor) HasRoleOption(
	_ context.Context, roleOption roleoption.Option,
) (bool, error) {
	_, ok := a.roleOptions[roleOption]
	return ok, nil
}

func (a *testAuthAccessor) UserHasAdminRole(
	_ context.Context, user username.SQLUsername,
) (bool, error) {
	_, ok := a.admins[user.Normalized()]
	return ok, nil
}

func (a *testAuthAccessor) HasAdminRole(ctx context.Context) (bool, error) {
	_, ok := a.admins[a.user.Normalized()]
	return ok, nil
}

func (a *testAuthAccessor) HasGlobalPrivilegeOrRoleOption(
	ctx context.Context, privilege privilege.Kind,
) (bool, error) {
	ok, err := a.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege, a.User())
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	maybeRoleOptionName := string(privilege.DisplayName())
	if roleOption, ok := roleoption.ByName[maybeRoleOptionName]; ok {
		return a.HasRoleOption(ctx, roleOption)
	}
	return false, nil
}

func (a *testAuthAccessor) MemberOfWithAdminOption(
	ctx context.Context, member username.SQLUsername,
) (map[username.SQLUsername]bool, error) {
	return a.members, nil
}

func (a *testAuthAccessor) User() username.SQLUsername {
	return a.user
}

func makeChangefeedPayload(owner string, tableIDs []descpb.ID) *jobspb.Payload {
	specs := make([]jobspb.ChangefeedTargetSpecification, len(tableIDs))
	for i, tableID := range tableIDs {
		specs[i] = jobspb.ChangefeedTargetSpecification{
			TableID: tableID,
		}
	}
	return &jobspb.Payload{
		Details: jobspb.WrapPayloadDetails(jobspb.ChangefeedDetails{
			TargetSpecifications: specs,
		}),
		UsernameProto: username.MakeSQLUsernameFromPreNormalizedString(owner).EncodeProto(),
	}
}

func makeBackupPayload(owner string) *jobspb.Payload {
	return &jobspb.Payload{
		Details:       jobspb.WrapPayloadDetails(jobspb.BackupDetails{}),
		UsernameProto: username.MakeSQLUsernameFromPreNormalizedString(owner).EncodeProto(),
	}
}

func TestAuthorization(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	t.Logf("random seed: %d", seed)

	for _, tc := range []struct {
		name string

		user                 username.SQLUsername
		roleOptions          map[roleoption.Option]struct{}
		members              map[username.SQLUsername]bool
		userPrivileges       map[userPrivilege]struct{}
		changeFeedPrivileges map[descpb.ID]struct{}
		droppedDescriptors   map[descpb.ID]struct{}
		admins               map[string]struct{}

		payload     *jobspb.Payload
		accessLevel jobsauth.AccessLevel
		userErr     error
	}{
		{
			name: "controljob-role-option-sufficient-for-non-admin-jobs",

			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ControlAccess,
		},
		{
			name: "controljob-role-option-sufficient-to-view-admin-jobs",
			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			admins:      map[string]struct{}{"user2": {}},
			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ViewAccess,
		},
		{
			name:        "users-access-their-own-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("user1"),
			accessLevel: jobsauth.ViewAccess,
		},
		{
			name:        "users-can-control-their-own-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("user1"),
			accessLevel: jobsauth.ControlAccess,
		},
		{
			name:        "users-can-control-their-roles-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("role1"),
			accessLevel: jobsauth.ControlAccess,
			members: map[username.SQLUsername]bool{
				username.MakeSQLUsernameFromPreNormalizedString("role1"): false,
			},
		},
		{
			name:        "admins-control-admin-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{"user2": {}, "user1": {}},
			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ControlAccess,
		},
		{
			name:                 "changefeed-privilege-on-all-tables",
			user:                 username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:          map[roleoption.Option]struct{}{},
			admins:               map[string]struct{}{},
			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}, 2: {}},

			payload:     makeChangefeedPayload("user2", []descpb.ID{0, 1, 2}),
			accessLevel: jobsauth.ControlAccess,
		},
		{
			name:                 "changefeed-privilege-on-some-tables",
			user:                 username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:          map[roleoption.Option]struct{}{},
			admins:               map[string]struct{}{},
			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}},

			payload:     makeChangefeedPayload("user2", []descpb.ID{0, 1, 2}),
			accessLevel: jobsauth.ControlAccess,
			userErr:     pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:                 "changefeed-priv-on-some-tables-with-dropped",
			user:                 username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:          map[roleoption.Option]struct{}{},
			admins:               map[string]struct{}{},
			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}},
			droppedDescriptors:   map[descpb.ID]struct{}{2: {}},

			payload:     makeChangefeedPayload("user2", []descpb.ID{0, 1, 2}),
			accessLevel: jobsauth.ControlAccess,
		},
		{
			name:   "viewjob-required-for-read-access",
			user:   username.MakeSQLUsernameFromPreNormalizedString("user1"),
			admins: map[string]struct{}{},

			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ViewAccess,
			userErr:     pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:           "viewjob-allows-read-access",
			user:           username.MakeSQLUsernameFromPreNormalizedString("user1"),
			userPrivileges: map[userPrivilege]struct{}{viewJobGlobalPrivilege: {}},
			admins:         map[string]struct{}{},

			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ViewAccess,
		},
		{
			name:           "viewjob-disallows-control-access",
			user:           username.MakeSQLUsernameFromPreNormalizedString("user1"),
			userPrivileges: map[userPrivilege]struct{}{viewJobGlobalPrivilege: {}},
			admins:         map[string]struct{}{},

			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ControlAccess,
			userErr:     pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name: "controljob-system-privilege-sufficient-for-non-admin-jobs",

			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			userPrivileges: map[userPrivilege]struct{}{
				controlJobGlobalPrivilege: {},
			},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ControlAccess,
		},
		{
			name: "controljob-system-privilege-sufficient-to-view-admin-jobs",
			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			userPrivileges: map[userPrivilege]struct{}{
				controlJobGlobalPrivilege: {},
			},
			admins:      map[string]struct{}{"user2": {}},
			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ViewAccess,
		},
		{
			name: "controljob-system-privilege-insufficient-to-control-admin-jobs",
			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			userPrivileges: map[userPrivilege]struct{}{
				controlJobGlobalPrivilege: {},
			},
			admins:      map[string]struct{}{"user2": {}},
			payload:     makeBackupPayload("user2"),
			accessLevel: jobsauth.ControlAccess,
			userErr:     pgerror.New(pgcode.InsufficientPrivilege, "admin"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testAuth := &testAuthAccessor{
				user:                 tc.user,
				roleOptions:          tc.roleOptions,
				userPrivileges:       tc.userPrivileges,
				changeFeedPrivileges: tc.changeFeedPrivileges,
				droppedDescriptors:   tc.droppedDescriptors,
				admins:               tc.admins,
				members:              tc.members,
				rand:                 rng,
			}

			ctx := context.Background()
			globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, testAuth)
			assert.NoError(t, err)
			err = jobsauth.Authorize(
				ctx, testAuth, 0,
				func(ctx context.Context) (*jobspb.Payload, error) { return tc.payload, nil },
				tc.payload.UsernameProto.Decode(), tc.payload.Type(), tc.accessLevel, globalPrivileges,
			)
			assert.Equal(t, pgerror.GetPGCode(tc.userErr), pgerror.GetPGCode(err))
		})
	}
}
