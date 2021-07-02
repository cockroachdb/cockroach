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
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestFixPrivilegesMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The SQL statements below are run on a 20.1.2 cluster, the ZONECONFIG bit
	// becomes the USAGE bit in versions >20.2 causing the privilege descriptor
	// to become corrupted since USAGE is not valid on tables and dbs.
	/*
	   CREATE USER testuser;
	   CREATE DATABASE db;
	   GRANT ZONECONFIG ON DATABASE db TO testuser;
	   USE db;
	   CREATE TABLE tb();
	   GRANT ZONECONFIG ON tb TO testuser;

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
	const descriptorStringsToInjectZoneConfigFixTest = `
122c0a026462103b1a240a090a0561646d696e10020a080a04726f6f7410020a0d0a087465737475736572108004
0afe010a027462183c203b28023a00422b0a05726f77696410011a0c08011040180030005014600020002a0e756e697175655f726f77696428293001480252480a077072696d617279100118012205726f776964300140004a10080010001a00200028003000380040005a007a020800800100880100900100980100a20106080012001800a8010060026a240a090a0561646d696e10020a080a04726f6f7410020a0d0a087465737475736572108004800101880103980100b201160a077072696d61727910001a05726f77696420012800b80101c20100e80100f2010408001200f801008002009202009a020a08d0b7a9c4fcdd9fc416b20200b80200c0021d
`

	// Test fixing a corrupted schema privilege descriptor after converting
	// from a database to a schema. The SQL statements below are run on a 20.2.5
	// cluster where ALTER DATABASE ... CONVERT TO SCHEMA did not correctly handle
	// privileges. The "schema" is corrupted due to testuser having SELECT
	// privilege on it.
	/*
	   CREATE DATABASE parent;
	   CREATE DATABASE schema;
	   CREATE USER TESTUSER;
	   GRANT SELECT ON DATABASE SCHEMA TO TESTUSER;
	   ALTER DATABASE schema CONVERT TO SCHEMA WITH PARENT parent;

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
	                            WHERE "parentID" = 0 AND name = 'parent'
	                       )
	                   OR "parentID" = 0 AND name = 'parent'
	           );
	*/
	const descriptorStringsToInjectSchemaTest = `
12410a06706172656e74103b1a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801220028023a0e0a06736368656d611204083d100040004a00
2241083b1206736368656d61183d222b0a090a0561646d696e10020a080a04726f6f7410020a0c0a08746573747573657210201204726f6f7418012a00300140004a00
`

	// Ensure types can also be upgraded. Types in practice should not be corrupted
	// and should always have an owner since they're created in 20.2 and onwards.
	// We still want to upgrade the Version of the privilege descriptor.
	// The following SQL statements were run in a 20.2.5 cluster.
	/*
	   CREATE DATABASE db;
	   USE db;
	   CREATE TYPE t AS ENUM();

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
	const descriptorStringsToInjectUserDefinedTypeTest = `
122d0a026462103d1a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f7418012200280140004a00
1a41083d101d1a0174203e2800403f480152006800722a0a090a0561646d696e10020a0b0a067075626c69631080040a080a04726f6f7410021204726f6f7418017a00
1a6a083d101d1a025f74203f28013a26080f100018003000381850df8d065a14081810001800300050de8d0660007a0410df8d0660004000480152006800722a0a090a0561646d696e10020a0b0a067075626c69631080040a080a04726f6f7410021204726f6f7418017a00
`

	testCases := []string{
		descriptorStringsToInjectZoneConfigFixTest,
		descriptorStringsToInjectSchemaTest,
		descriptorStringsToInjectUserDefinedTypeTest,
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
						BinaryVersionOverride:          clusterversion.ByKey(clusterversion.FixDescriptors - 1),
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
			clusterversion.ByKey(clusterversion.FixDescriptors).String())
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
				if privilegeDesc.Version < descpb.Version21_2 {
					return errors.New("privilege descriptors must have at least Version21_2")
				}

			}
			return nil
		})

		require.NoError(t, err)
		tc.Stopper().Stop(ctx)
	}
}
