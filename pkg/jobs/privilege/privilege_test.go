// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilege_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	jobsprivilege "github.com/cockroachdb/cockroach/pkg/jobs/privilege"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

type testAuthAccessor struct {
	user username.SQLUsername

	// set of role options that the user has
	roleOptions map[roleoption.Option]struct{}

	// set of descriptors which the user has privilege.CHANGEFEED on
	changeFeedPrivileges map[descpb.ID]struct{}
	// set of descriptors which are dropped
	droppedDescriptors map[descpb.ID]struct{}

	// set of all usernames who are admins
	admins map[string]struct{}

	rand *rand.Rand
}

var _ jobsprivilege.AuthorizationAccessor = &testAuthAccessor{}

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

func TestPrivileges(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	t.Logf("random seed: %d", seed)

	for _, tc := range []struct {
		name string

		user                 username.SQLUsername
		roleOptions          map[roleoption.Option]struct{}
		changeFeedPrivileges map[descpb.ID]struct{}
		droppedDescriptors   map[descpb.ID]struct{}
		admins               map[string]struct{}

		payload *jobspb.Payload
		flags   []jobsprivilege.AccessFlag

		canAccess bool
		userErr   error
	}{
		{
			name: "controljob-sufficient-for-non-admin-jobs",

			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			admins:  map[string]struct{}{},
			payload: makeBackupPayload("user2"),
			flags:   []jobsprivilege.AccessFlag{jobsprivilege.UserCanAccessWithControlJob},

			canAccess: true,
		},
		{
			name: "no-controljob-flag",
			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			payload:   makeBackupPayload("user2"),
			canAccess: false,
			userErr:   pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name: "controljob-insufficient-for-admin-jobs",
			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			admins:    map[string]struct{}{"user2": {}},
			payload:   makeBackupPayload("user2"),
			flags:     []jobsprivilege.AccessFlag{jobsprivilege.UserCanAccessWithControlJob},
			canAccess: false,
			userErr:   pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:        "users-access-their-own-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("user1"),
			flags:       []jobsprivilege.AccessFlag{jobsprivilege.UserCanAccessOwnJob},
			canAccess:   true,
		},
		{
			name:        "users-cannot-access-their-own-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},
			payload:     makeBackupPayload("user1"),
			canAccess:   false,
			userErr:     pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:        "admins-see-admin-jobs",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{"user2": {}, "user1": {}},
			payload:     makeBackupPayload("user2"),
			canAccess:   true,
		},
		{
			name:                 "changefeed-privilege-on-all-tables",
			user:                 username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:          map[roleoption.Option]struct{}{},
			admins:               map[string]struct{}{},
			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}, 2: {}},

			payload:   makeChangefeedPayload("user2", []descpb.ID{0, 1, 2}),
			canAccess: true,
		},
		{
			name:                 "changefeed-privilege-on-some-tables",
			user:                 username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:          map[roleoption.Option]struct{}{},
			admins:               map[string]struct{}{},
			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}},

			payload:   makeChangefeedPayload("user2", []descpb.ID{0, 1, 2}),
			canAccess: false,
			userErr:   pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:                 "changefeed-priv-on-some-tables-with-dropped",
			user:                 username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:          map[roleoption.Option]struct{}{},
			admins:               map[string]struct{}{},
			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}},
			droppedDescriptors:   map[descpb.ID]struct{}{2: {}},

			payload:   makeChangefeedPayload("user2", []descpb.ID{0, 1, 2}),
			canAccess: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testAuth := &testAuthAccessor{
				user:                 tc.user,
				roleOptions:          tc.roleOptions,
				changeFeedPrivileges: tc.changeFeedPrivileges,
				droppedDescriptors:   tc.droppedDescriptors,
				admins:               tc.admins,
				rand:                 rng,
			}

			ctx := context.Background()
			err := jobsprivilege.Authorize(ctx, testAuth, 0, tc.payload, tc.flags...)
			assert.Equal(t, pgerror.GetPGCode(tc.userErr), pgerror.GetPGCode(err))
		})
	}
}
