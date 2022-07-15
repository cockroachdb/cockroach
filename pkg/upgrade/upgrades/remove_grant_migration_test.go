// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// nolint
func TestRemoveGrantMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test when a user holds privileges on a database, schema, and table
	/*
					   CREATE USER testuser;
						 CREATE DATABASE db;
						 GRANT GRANT, CREATE ON DATABASE db TO testuser;
						 USE db;
						 CREATE SCHEMA s;
				     GRANT GRANT, CREATE ON SCHEMA s TO testuser;
			       CREATE TABLE t1();
						 GRANT GRANT, SELECT ON TABLE t1 TO testuser;
						 CREATE TYPE type1 AS ENUM ('v1');
		         GRANT GRANT, USAGE ON TYPE type1 TO testuser;

					   	The descriptors were then extracted using:

					   	SELECT encode(descriptor, 'hex') AS descriptor
					     FROM system.descriptor
					    WHERE id
					          IN (
					               SELECT id
					                 FROM system.namespace
					                WHERE "parentID"
					                      = (
					                           SELECT id
					                             FROM system.namespace
					                            WHERE "parentID" = 0 AND name = 'db'
					                       )
					                   OR "parentID" = 0 AND name = 'db'
					           );

	*/
	const privilegeTest = `
12480a026462103b1a2b0a090a0561646d696e10020a080a04726f6f7410020a0c0a0874657374757365721014120464656d6f1802220028033a090a01731204083c100040004a005a00
2246083b120173183c22350a090a0561646d696e10020a080a0464656d6f10020a080a04726f6f7410020a0c0a0874657374757365721014120464656d6f18022a00300240004a00
0aaf020a027431183d203b28023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a077072696d617279100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a350a090a0561646d696e10020a080a0464656d6f10020a080a04726f6f7410020a0c0a0874657374757365721034120464656d6f1802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08a8fb81e6d2aab8fa16b20200b80200c0021dc80200e00200f00200
1a6b083b101d1a057479706531203e2800320b0a01801202763118002000403f48025200680072430a090a0561646d696e10020a080a0464656d6f10020a0b0a067075626c69631080040a080a04726f6f7410020a0d0a087465737475736572109004120464656d6f18027a00
1a78083b101d1a065f7479706531203f28013a26080f100018003000381850df8d065a14081810001800300050de8d0660007a0410df8d066000400048015200680072340a090a0561646d696e10020a080a0464656d6f10020a0b0a067075626c69631080040a080a04726f6f741002120464656d6f18027a00
`

	testCases := map[string]string{
		"privilege_test": privilegeTest,
	}
	for name, descriptorStringsToInject := range testCases {
		t.Run(name, func(t *testing.T) {
			var descriptorsToInject []*descpb.Descriptor
			for _, s := range strings.Split(strings.TrimSpace(descriptorStringsToInject), "\n") {
				encoded, err := hex.DecodeString(s)
				require.NoError(t, err)
				var desc descpb.Descriptor
				require.NoError(t, protoutil.Unmarshal(encoded, &desc))
				tableDescriptor, databaseDescriptor, typeDescriptor, schemaDescriptor, fnDescriptor := descpb.FromDescriptorWithMVCCTimestamp(
					&desc,
					hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
				)
				requireGrant := func(privilegeDescriptor *catpb.PrivilegeDescriptor) {
					for _, user := range privilegeDescriptor.Users {
						if user.User().Normalized() == "test_user" {
							require.True(t, privilege.DEPRECATEDGRANT.IsSetIn(user.Privileges))
						}
					}
				}
				if databaseDescriptor != nil {
					requireGrant(databaseDescriptor.Privileges)
				}
				if schemaDescriptor != nil {
					requireGrant(schemaDescriptor.Privileges)
				}
				if tableDescriptor != nil {
					requireGrant(tableDescriptor.Privileges)
				}
				if typeDescriptor != nil {
					requireGrant(typeDescriptor.Privileges)
				}
				if fnDescriptor != nil {
					requireGrant(fnDescriptor.Privileges)
				}
				descriptorsToInject = append(descriptorsToInject, &desc)
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: make(chan struct{}),
							BinaryVersionOverride:          clusterversion.ByKey(clusterversion.RemoveGrantPrivilege - 1), // changed cluster version
						},
					},
				},
			})

			db := tc.ServerConn(0)
			require.NoError(t, sqlutils.InjectDescriptors(
				ctx, db, descriptorsToInject, true, /* force */
			))

			_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
				clusterversion.ByKey(clusterversion.RemoveGrantPrivilege).String())
			require.NoError(t, err)

			err = sql.TestingDescsTxn(ctx, tc.Server(0), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
				// Avoid running validation on the descriptors.
				cat, err := col.Direct().GetCatalogUnvalidated(ctx, txn)
				if err != nil {
					return err
				}
				for _, desc := range cat.OrderedDescriptors() {
					privilegeDesc := desc.GetPrivileges()
					for _, u := range privilegeDesc.Users {
						if privilege.DEPRECATEDGRANT.IsSetIn(u.Privileges) {
							return errors.Newf(
								"grant not removed properly for %s, Privileges: %d, Grant Option: %d",
								u.User(), u.Privileges, u.WithGrantOption)
						}
					}
				}
				return nil
			})
			require.NoError(t, err)
			tc.Stopper().Stop(ctx)
		})
	}
}
