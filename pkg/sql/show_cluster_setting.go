// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func (p *planner) showVersionSetting(
	ctx context.Context, st *cluster.Settings, s *settings.VersionSetting, name string,
) (string, error) {
	var res string
	// For the version setting we show the value from the KV store and
	// additionally wait for the local setting instance to have observed the
	// value as well (gets updated through the `BumpClusterVersion` RPC). This
	// makes sure that cluster version bumps become visible immediately while at
	// the same time guaranteeing that a node reporting a certain version has
	// also processed the corresponding version bump (which is important as only
	// then does the node update its persisted state; see #22796).
	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("show cluster setting %s", name), 2*time.Minute,
		func(ctx context.Context) error {
			tBegin := timeutil.Now()

			// The (slight ab)use of WithMaxAttempts achieves convenient context cancellation.
			return retry.WithMaxAttempts(ctx, retry.Options{}, math.MaxInt32, func() error {
				return p.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					datums, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
						ctx, "read-setting",
						txn,
						sessiondata.InternalExecutorOverride{User: security.RootUserName()},
						"SELECT value FROM system.settings WHERE name = $1", name,
					)
					if err != nil {
						return err
					}
					var kvRawVal []byte
					if len(datums) != 0 {
						dStr, ok := datums[0].(*tree.DString)
						if !ok {
							return errors.New("the existing value is not a string")
						}
						kvRawVal = []byte(string(*dStr))
					} else if !p.execCfg.Codec.ForSystemTenant() {
						// The tenant clusters in 20.2 did not ever initialize this value
						// and utilized this hard-coded value instead. In 21.1, the builtin
						// which creates tenants sets up the cluster version state. It also
						// is set when the version is upgraded.
						tenantDefaultVersion := clusterversion.ClusterVersion{
							Version: clusterversion.ByKey(clusterversion.V20_2),
						}
						encoded, err := protoutil.Marshal(&tenantDefaultVersion)
						if err != nil {
							return errors.WithAssertionFailure(err)
						}
						kvRawVal = encoded
					} else {
						// There should always be a version saved; there's a migration
						// populating it.
						return errors.AssertionFailedf("no value found for version setting")
					}

					localRawVal := []byte(s.Get(&st.SV))
					if !bytes.Equal(localRawVal, kvRawVal) {
						return errors.Errorf(
							"value differs between local setting (%v) and KV (%v); try again later (%v after %s)",
							localRawVal, kvRawVal, ctx.Err(), timeutil.Since(tBegin))
					}

					val, err := s.Decode(kvRawVal)
					if err != nil {
						return err
					}

					res = val.String()
					return nil
				})
			})
		}); err != nil {
		return "", err
	}

	return res, nil
}

func (p *planner) ShowClusterSetting(
	ctx context.Context, n *tree.ShowClusterSetting,
) (planNode, error) {
	name := strings.ToLower(n.Name)
	st := p.ExecCfg().Settings
	val, ok := settings.Lookup(name, settings.LookupForLocalAccess)
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}

	if err := checkPrivilegesForSetting(ctx, p, name, "show"); err != nil {
		return nil, err
	}

	var dType *types.T
	switch val.(type) {
	case *settings.IntSetting:
		dType = types.Int
	case *settings.StringSetting, *settings.ByteSizeSetting, *settings.VersionSetting, *settings.EnumSetting:
		dType = types.String
	case *settings.BoolSetting:
		dType = types.Bool
	case *settings.FloatSetting:
		dType = types.Float
	case *settings.DurationSetting:
		dType = types.Interval
	case *settings.DurationSettingWithExplicitUnit:
		dType = types.Interval
	default:
		return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
	}

	columns := colinfo.ResultColumns{{Name: name, Typ: dType}}
	return &delayedNode{
		name:    "SHOW CLUSTER SETTING " + name,
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			var d tree.Datum
			switch s := val.(type) {
			case *settings.IntSetting:
				d = tree.NewDInt(tree.DInt(s.Get(&st.SV)))
			case *settings.StringSetting:
				d = tree.NewDString(s.String(&st.SV))
			case *settings.BoolSetting:
				d = tree.MakeDBool(tree.DBool(s.Get(&st.SV)))
			case *settings.FloatSetting:
				d = tree.NewDFloat(tree.DFloat(s.Get(&st.SV)))
			case *settings.DurationSetting:
				d = &tree.DInterval{Duration: duration.MakeDuration(s.Get(&st.SV).Nanoseconds(), 0, 0)}
			case *settings.DurationSettingWithExplicitUnit:
				d = &tree.DInterval{Duration: duration.MakeDuration(s.Get(&st.SV).Nanoseconds(), 0, 0)}
			case *settings.EnumSetting:
				d = tree.NewDString(s.String(&st.SV))
			case *settings.ByteSizeSetting:
				d = tree.NewDString(s.String(&st.SV))
			case *settings.VersionSetting:
				valStr, err := p.showVersionSetting(ctx, st, s, name)
				if err != nil {
					return nil, err
				}
				d = tree.NewDString(valStr)
			default:
				return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
			}

			v := p.newContainerValuesNode(columns, 0)
			if _, err := v.rows.AddRow(ctx, tree.Datums{d}); err != nil {
				v.rows.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}
