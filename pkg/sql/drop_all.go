// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// featureBackupEnabled is used to enable and disable the BACKUP feature.
var featureExperimentalDropAllTablesEnabled = settings.RegisterBoolSetting(
	"feature.experimental_drop_all_tables.enabled",
	"set to true to enable the experimental DROP ALL feature that deletes all tables",
	false,
).WithPublic()

type dropAllNode struct {
	n  *tree.DropAll
}

var _ planNode = &dropAllNode{n: nil}

func (p *planner) DropAll(ctx context.Context, n *tree.DropAll) (planNode, error) {
	node := &dropAllNode{
		n:  n,
	}
	return node, nil
}

func (n *dropAllNode) startExec(params runParams) error {
	if err := params.p.dropAllImpl(params.ctx); err != nil {
		return err
	}

	// Log an event.

	return nil
}

// dropTypeImpl does the work of dropping a type and everything that depends on it.
func (p *planner) dropAllImpl(ctx context.Context) error {
	// TODO: Make this a session var informed by a cluster setting.
	if !featureExperimentalDropAllTablesEnabled.Get(&p.execCfg.Settings.SV) {
		return errors.WithHint(
			errors.WithIssueLink(
				errors.Newf("drop all tables is only supported experimentally"),
				errors.IssueLink{IssueURL: build.MakeIssueURL(46260)},
			),
			"You can enable DROP ALL by running `SET CLUSTER SETTING feature.experimental_drop_all_tables.enabled = 'on'`.",
		)
	}

	// First clear all user data.
	{
		startKey := keys.TODOSQLCodec.TablePrefix(keys.MinUserDescID)
		sp := roachpb.Span{Key: startKey, EndKey: keys.MaxKey}
		b := &kv.Batch{}
		b.AddRawRequest(&roachpb.ClearRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    sp.Key,
				EndKey: sp.EndKey,
			},
		})
		if err := p.txn.DB().Run(ctx, b); err != nil {
			return errors.Wrap(err, "dropping all the data")
		}
	}

	codec := p.ExecCfg().Codec
	if err := p.txn.SetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
		return err
	}

	execCfg := p.execCfg

	// TODO: Users can still query the dropped tables (although they will not
	// appear in queries such as SHOW TABLES). The cache holding these objects
	// need to be cleared
	return descs.Txn(ctx, execCfg.Settings, execCfg.LeaseManager, execCfg.InternalExecutor,
		execCfg.DB, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			b := &kv.Batch{}

			allDescs, err := descsCol.GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}

			for _, desc := range allDescs {
				if desc.GetID() == keys.SystemDatabaseID || desc.GetParentID() == keys.SystemDatabaseID {
					continue
				}

				var mutTable *tabledesc.Mutable
				if tableDesc, ok := desc.(catalog.TableDescriptor); ok {
					if immutableTable := tableDesc.TableDesc(); immutableTable != nil {
						mutTable = tabledesc.NewExistingMutable(*immutableTable)
					} else {
						return errors.New("expected a table")
					}
				} else {
					// We only want to delete tables.
					continue
				}

				// Remove any associated comments.
				if err := p.removeTableComments(ctx, desc.GetID()); err != nil {
					return err
				}

				// Delete the zone config entry.
				if codec.ForSystemTenant() {
					zoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(desc.GetID()))
					b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
				}

				mutTable.Version = mutTable.GetVersion() + 1
				if err := descsCol.WriteDescToBatch(
					ctx, false /* kvTrace */, mutTable, b,
				); err != nil {
					return err
				}

				// Delete the descriptor.
				b.Del(catalogkeys.MakeDescMetadataKey(codec, desc.GetID()))

				// Remove from namespace.
				catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
					ctx,
					b,
					keys.SystemSQLCodec,
					desc.GetParentID(),
					desc.GetParentSchemaID(),
					desc.GetName(),
					false, /* kvTrace */
				)
			}

			return p.txn.Run(ctx, b)
		})
}

func (n *dropAllNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropAllNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropAllNode) Close(ctx context.Context)           {}
func (n *dropAllNode) ReadingOwnWrites()                   {}
