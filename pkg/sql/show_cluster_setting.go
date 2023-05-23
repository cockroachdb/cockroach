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
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// getCurrentEncodedVersionSettingValue returns the encoded value of
// the version setting. The caller is responsible for decoding the
// value to transform it to a user-facing string.
func (p *planner) getCurrentEncodedVersionSettingValue(
	ctx context.Context, s *settings.VersionSetting, name string,
) (string, error) {
	st := p.ExecCfg().Settings
	var res string
	// For the version setting we show the value from the KV store and
	// additionally wait for the local setting instance to have observed the
	// value as well (gets updated through the `BumpClusterVersion` RPC). This
	// makes sure that cluster version bumps become visible immediately while at
	// the same time guaranteeing that a node reporting a certain version has
	// also processed the corresponding version bump (which is important as only
	// then does the node update its persisted state; see #22796).
	if err := timeutil.RunWithTimeout(ctx, fmt.Sprintf("show cluster setting %s", name), 2*time.Minute,
		func(ctx context.Context) error {
			tBegin := timeutil.Now()

			// The (slight ab)use of WithMaxAttempts achieves convenient context cancellation.
			return retry.WithMaxAttempts(ctx, retry.Options{}, math.MaxInt32, func() error {
				return p.execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					datums, err := txn.QueryRowEx(
						ctx, "read-setting",
						txn.KV(),
						sessiondata.RootUserSessionDataOverride,
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
							Version: roachpb.Version{Major: 20, Minor: 2},
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
					if err := checkClusterSettingValuesAreEquivalent(
						localRawVal, kvRawVal,
					); err != nil {
						// NB: errors.Wrapf(nil, ...) returns nil.
						// nolint:errwrap
						return errors.WithHintf(err, "try again later (%v after %v)",
							ctx.Err(), timeutil.Since(tBegin))
					}
					res = string(kvRawVal)
					return nil
				})
			})
		}); err != nil {
		return "", err
	}

	return res, nil
}

// checkClusterSettingValuesAreEquivalent returns an error if the cluster
// setting values are not equivalent. Equivalent cluster setting values
// are either the byte-for-byte identical, or the local value is the successor
// to the kv value and the local value is a fence version.
//
// The in-memory version gets pushed to the fence but the fence is not persisted,
// so, while migrations are ongoing, we won't see these values match. In practice
// this is a problem these days because the migrations take a long time.
func checkClusterSettingValuesAreEquivalent(localRawVal, kvRawVal []byte) error {
	if bytes.Equal(localRawVal, kvRawVal) {
		return nil
	}
	type cv = clusterversion.ClusterVersion
	maybeDecodeVersion := func(data []byte) (cv, any, bool) {
		if len(data) == 0 {
			return cv{}, data, false
		}
		var v cv
		if err := protoutil.Unmarshal(data, &v); err != nil {
			return cv{}, data, false
		}
		return v, v, true
	}
	decodedLocal, localVal, localOk := maybeDecodeVersion(localRawVal)
	decodedKV, kvVal, kvOk := maybeDecodeVersion(kvRawVal)
	if localOk && kvOk && decodedLocal.Internal%2 == 1 /* isFence */ {
		predecessor := decodedLocal
		predecessor.Internal--
		if predecessor.Equal(decodedKV) {
			return nil
		}
	}
	return errors.Errorf(
		"value differs between local setting (%v) and KV (%v)",
		localVal, kvVal)
}

func (p *planner) ShowClusterSetting(
	ctx context.Context, n *tree.ShowClusterSetting,
) (planNode, error) {
	name := strings.ToLower(n.Name)
	setting, ok := settings.LookupForLocalAccess(name, p.ExecCfg().Codec.ForSystemTenant())
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}

	if err := checkPrivilegesForSetting(ctx, p, name, "show"); err != nil {
		return nil, err
	}

	if strings.HasPrefix(n.Name, "sql.defaults") {
		p.BufferClientNotice(
			ctx,
			errors.WithHintf(
				pgnotice.Newf("using global default %s is not recommended", n.Name),
				"use the `ALTER ROLE ... SET` syntax to control session variable defaults at a finer-grained level. See: %s",
				docs.URL("alter-role.html#set-default-session-variable-values-for-a-role"),
			),
		)
	}

	columns, err := getShowClusterSettingPlanColumns(setting, name)
	if err != nil {
		return nil, err
	}

	return planShowClusterSetting(setting, name, columns,
		func(ctx context.Context, p *planner) (bool, string, error) {
			if verSetting, ok := setting.(*settings.VersionSetting); ok {
				encoded, err := p.getCurrentEncodedVersionSettingValue(ctx, verSetting, name)
				return true, encoded, err
			}
			return true, setting.Encoded(&p.ExecCfg().Settings.SV), nil
		},
	)
}

func getShowClusterSettingPlanColumns(
	val settings.NonMaskedSetting, name string,
) (colinfo.ResultColumns, error) {
	var dType *types.T
	switch val.(type) {
	case *settings.IntSetting:
		dType = types.Int
	case *settings.StringSetting, *settings.ByteSizeSetting, *settings.VersionSetting, *settings.EnumSetting, *settings.ProtobufSetting:
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
	return colinfo.ResultColumns{{Name: name, Typ: dType}}, nil
}

func planShowClusterSetting(
	val settings.NonMaskedSetting,
	name string,
	columns colinfo.ResultColumns,
	getEncodedValue func(ctx context.Context, p *planner) (bool, string, error),
) (planNode, error) {
	return &delayedNode{
		name:    "SHOW CLUSTER SETTING " + name,
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			isNotNull, encoded, err := getEncodedValue(ctx, p)
			if err != nil {
				return nil, err
			}

			var d tree.Datum
			d = tree.DNull
			if isNotNull {
				switch s := val.(type) {
				case *settings.IntSetting:
					v, err := s.DecodeValue(encoded)
					if err != nil {
						return nil, err
					}
					d = tree.NewDInt(tree.DInt(v))
				case *settings.StringSetting, *settings.EnumSetting,
					*settings.ByteSizeSetting, *settings.VersionSetting, *settings.ProtobufSetting:
					v, err := val.DecodeToString(encoded)
					if err != nil {
						return nil, err
					}
					d = tree.NewDString(v)
				case *settings.BoolSetting:
					v, err := s.DecodeValue(encoded)
					if err != nil {
						return nil, err
					}
					d = tree.MakeDBool(tree.DBool(v))
				case *settings.FloatSetting:
					v, err := s.DecodeValue(encoded)
					if err != nil {
						return nil, err
					}
					d = tree.NewDFloat(tree.DFloat(v))
				case *settings.DurationSetting:
					v, err := s.DecodeValue(encoded)
					if err != nil {
						return nil, err
					}
					d = &tree.DInterval{Duration: duration.MakeDuration(v.Nanoseconds(), 0, 0)}
				case *settings.DurationSettingWithExplicitUnit:
					v, err := s.DecodeValue(encoded)
					if err != nil {
						return nil, err
					}
					d = &tree.DInterval{Duration: duration.MakeDuration(v.Nanoseconds(), 0, 0)}
				default:
					return nil, errors.AssertionFailedf("unknown setting type for %s: %s (%T)", name, val.Typ(), val)
				}
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
