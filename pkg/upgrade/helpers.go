// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrade

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/errors"
)

// BumpSystemDatabaseSchemaVersion bumps the SystemDatabaseSchemaVersion
// field for the system database descriptor. It is called after every upgrade
// step that has an associated migration, and when upgrading to the final
// clusterversion for a release.
func BumpSystemDatabaseSchemaVersion(
	ctx context.Context, version roachpb.Version, descDB descs.DB,
) error {
	if err := descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		systemDBDesc, err := txn.Descriptors().MutableByID(txn.KV()).Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		if sv := systemDBDesc.GetSystemDatabaseSchemaVersion(); sv != nil {
			if version.Less(*sv) {
				return errors.AssertionFailedf(
					"new system schema version (%#v) is lower than previous system schema version (%#v)",
					version,
					*sv,
				)
			} else if version.Equal(sv) {
				return nil
			}
		}
		systemDBDesc.SystemDatabaseSchemaVersion = &version
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, systemDBDesc, txn.KV())
	}); err != nil {
		return err
	}
	return nil
}
