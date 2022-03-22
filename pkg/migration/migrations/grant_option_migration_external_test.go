// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestGrantOptionMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test when a user holds privileges on a database, schema, and table
	/*
			   CREATE USER testuser;
				 CREATE DATABASE db;
				 GRANT CREATE ON DATABASE db TO testuser;
				 USE db;
				 CREATE SCHEMA s;
				 CREATE TABLE t1();
		     GRANT CREATE ON SCHEMA s TO testuser;
				 GRANT INSERT, SELECT ON TABLE t1 TO testuser;

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
	const objectTest = `
12480a02646210341a2b0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210041204726f6f741802220028033a090a017312040835100040004a005a00
223c08341201731835222b0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210041204726f6f7418022a00300240004a00
0aa5020a0274311836203428023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a077072696d617279100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a2b0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210641204726f6f741802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08e0bea485c6bbede616b20200b80200c0021dc80200e00200f00200
`

	// Check that descriptors for multiple users are pulled and updated when
	// creating schemas and tables within those schemas
	/*
		   	CREATE USER testuser;
				CREATE USER testuser2;
				CREATE DATABASE db;
				USE db;
				GRANT ALL PRIVILEGES ON DATABASE db TO testuser;
				CREATE schema s;
				CREATE table s.t1();
				CREATE table s.t2();
				GRANT GRANT, CREATE ON SCHEMA s TO testuser2;
				GRANT SELECT, DELETE ON TABLE s.t1 TO testuser2;
				GRANT ALL PRIVILEGES ON TABLE s.t2 TO testuser2;

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
	const multipleUsersTest = `
12480a02646210371a2b0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210021204726f6f741802220028033a090a017312040838100040004a005a00
224b08371201731838223a0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210020a0d0a0974657374757365723210141204726f6f7418022a00300240004a00
0ab5020a0274311839203728023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a077072696d617279100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a3b0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210020a0e0a0974657374757365723210a0011204726f6f741802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08c8c9daf5dabdede616b20200b80200c00238c80200e00200f00200
0ab4020a027432183a203728023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a077072696d617279100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a3a0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210020a0d0a0974657374757365723210021204726f6f741802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08c8c7f884dcbdede616b20200b80200c00238c80200e00200f00200
`

	// Test the migration for types
	/*
			   CREATE USER testuser;
				 CREATE DATABASE db;
				 USE db;
				 CREATE schema s;
				 CREATE TYPE ty AS ENUM();
		     CREATE TYPE s.ty2 AS ENUM();
				 GRANT USAGE ON TYPE ty TO testuser;
				 GRANT ALL PRIVILEGES ON TYPE s.ty2 TO testuser;

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
	const typesTest = `
123a0a026462103b1a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741802220028023a090a01731204083c100040004a005a00
222e083b120173183c221d0a090a0561646d696e10020a080a04726f6f7410021204726f6f7418022a00300140004a00
1a51083b101d1a027479203d2800403e48025200680072390a090a0561646d696e10020a0b0a067075626c69631080040a080a04726f6f7410020a0d0a0874657374757365721080041204726f6f7418027a00
1a6b083b101d1a035f7479203e28013a26080f100018003000381850de8d065a14081810001800300050dd8d0660007a0410de8d0660004000480152006800722a0a090a0561646d696e10020a0b0a067075626c69631080040a080a04726f6f7410021204726f6f7418027a00
1a51083b103c1a03747932203f2800404048025200680072380a090a0561646d696e10020a0b0a067075626c69631080040a080a04726f6f7410020a0c0a08746573747573657210021204726f6f7418027a00
1a6c083b103c1a045f747932204028013a26080f100018003000381850e08d065a14081810001800300050df8d0660007a0410e08d0660004000480152006800722a0a090a0561646d696e10020a0b0a067075626c69631080040a080a04726f6f7410021204726f6f7418027a00
`

	testCases := map[string]string{
		"object_test":       objectTest,
		"multipleUsersTest": multipleUsersTest,
		"typesTest":         typesTest,
	}
	for name, descriptorStringsToInject := range testCases {
		t.Run(name, func(t *testing.T) {
			var descriptorsToInject []*descpb.Descriptor
			for _, s := range strings.Split(strings.TrimSpace(descriptorStringsToInject), "\n") {
				encoded, err := hex.DecodeString(s)
				require.NoError(t, err)
				var desc descpb.Descriptor
				require.NoError(t, protoutil.Unmarshal(encoded, &desc))
				descriptorsToInject = append(descriptorsToInject, &desc)
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: make(chan struct{}),
							BinaryVersionOverride:          clusterversion.ByKey(clusterversion.ValidateGrantOption - 1), // changed cluster version
						},
					},
				},
			})

			db := tc.ServerConn(0)
			require.NoError(t, sqlutils.InjectDescriptors(
				ctx, db, descriptorsToInject, true, /* force */
			))

			_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
				clusterversion.ByKey(clusterversion.ValidateGrantOption).String())
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
						if privilege.GRANT.IsSetIn(u.Privileges) || privilege.ALL.IsSetIn(u.Privileges) {
							if u.UserProto.Decode().IsAdminRole() || u.UserProto.Decode().IsRootUser() || u.UserProto.Decode().IsNodeUser() {
								continue
							}

							if u.Privileges != u.WithGrantOption {
								return errors.Newf(
									"grant options not updated properly for %s, Privileges: %d, Grant Option: %d",
									u.User(), u.Privileges, u.WithGrantOption)
							}
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
