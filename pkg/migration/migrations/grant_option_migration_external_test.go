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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
124e0a02646210341a310a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572100418001204726f6f741802220028033a090a017312040835100040004a005a00
22420834120173183522310a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572100418001204726f6f7418022a00300240004a00
0aab020a0274311836203428023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a0774315f706b6579100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a310a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572106418001204726f6f741802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08d8bfcfc594a985dc16b20200b80200c0021dc80200e00200f00200
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
124e0a02646210341a310a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572100218021204726f6f741802220028033a090a017312040835100040004a005a00
22530834120173183522420a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572100218020a0f0a09746573747573657232101418001204726f6f7418022a00300240004a00
0abd020a0274311836203428023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a0774315f706b6579100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a430a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572100218020a100a0974657374757365723210a00118001204726f6f741802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08b8b091afa2d484dc16b20200b80200c00235c80200e00200f00200
0abc020a0274321837203428023a00423a0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001680070007800800100880100980100480252500a0774325f706b6579100118012205726f776964300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a420a0b0a0561646d696e100218020a0a0a04726f6f74100218020a0e0a087465737475736572100218020a0f0a09746573747573657232100218001204726f6f741802800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08d8efb894b8d484dc16b20200b80200c00235c80200e00200f00200
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
123e0a02646210341a210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f741802220028023a090a017312040835100040004a005a00
22320834120173183522210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418022a00300140004a00
1a590834101d1a02747920362800403748025200680072410a0b0a0561646d696e100218020a0d0a067075626c696310800418000a0a0a04726f6f74100218020a0f0a08746573747573657210800418001204726f6f7418027a00
1a710834101d1a035f7479203728013a26080f100018003000381850d78d065a14081810001800300050d68d0660007a0410d78d066000400048015200680072300a0b0a0561646d696e100218020a0d0a067075626c696310800418000a0a0a04726f6f74100218021204726f6f7418027a00
1a59083410351a0374793220382800403948025200680072400a0b0a0561646d696e100218020a0d0a067075626c696310800418000a0a0a04726f6f74100218020a0e0a087465737475736572100218001204726f6f7418027a00
1a72083410351a045f747932203928013a26080f100018003000381850d98d065a14081810001800300050d88d0660007a0410d98d066000400048015200680072300a0b0a0561646d696e100218020a0d0a067075626c696310800418000a0a0a04726f6f74100218021204726f6f7418027a00
`

	testCases := []string{
		objectTest,
		multipleUsersTest,
		typesTest,
	}
	for _, descriptorStringsToInject := range testCases {
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
						DisableAutomaticVersionUpgrade: 1,
						BinaryVersionOverride:          clusterversion.ByKey(clusterversion.ValidateGrantOption - 1), // changed cluster version
					},
				},
			},
		})

		db := tc.ServerConn(0)
		kvDB := tc.Server(0).DB()
		require.NoError(t, sqlutils.InjectDescriptors(
			ctx, db, descriptorsToInject, true, /* force */
		))

		_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
			clusterversion.ByKey(clusterversion.ValidateGrantOption).String())
		require.NoError(t, err)

		err = kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// GetAllDescriptors calls PrivilegeDescriptor.Validate, all the invalid
			// descriptors should be updated.
			descs, err := catalogkv.GetAllDescriptors(ctx, txn, keys.SystemSQLCodec, false /* shouldRunPostDeserializationChanges */)
			if err != nil {
				return err
			}

			for _, desc := range descs {
				privilegeDesc := desc.GetPrivileges()
				for _, u := range privilegeDesc.Users {
					if privilege.GRANT.IsSetIn(u.Privileges) || privilege.ALL.IsSetIn(u.Privileges) {
						if u.Privileges != u.WithGrantOption {
							return errors.New(fmt.Sprintf("grant options not updated properly for %d, Privileges: %d, Grant Option: %d", u.User(), u.Privileges, u.WithGrantOption))
						}
					}
				}

			}
			return nil
		})

		require.NoError(t, err)
		tc.Stopper().Stop(ctx)
	}
}
