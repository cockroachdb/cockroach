// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuppb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/stretchr/testify/require"
)

func TestUpgradeTenantDescriptors(t *testing.T) {
	t.Run("prefers Tenants over DeprecatedTenants", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					ID: 5,
				}},
			},
			TenantsDeprecated: []mtinfopb.ProtoInfo{
				{DeprecatedID: 6},
			},
		}
		require.NoError(t, manifest.UpgradeTenantDescriptors())
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{
				{SQLInfo: mtinfopb.SQLInfo{ID: 5}},
			}, manifest.Tenants)

	})
	t.Run("returns DeprecatedTenants when Tenants is empty", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			TenantsDeprecated: []mtinfopb.ProtoInfo{{
				DeprecatedID:        6,
				DeprecatedDataState: mtinfopb.ProtoInfo_READY,
			}},
		}
		require.NoError(t, manifest.UpgradeTenantDescriptors())
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					ID:        6,
					DataState: mtinfopb.DataStateReady,
				},
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        6,
					DeprecatedDataState: mtinfopb.ProtoInfo_READY,
				}},
			}, manifest.Tenants)

	})
	t.Run("copies ProtoInfo fields to SQLInfo with state ADD", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_ADD,
				}},
			},
		}
		require.NoError(t, manifest.UpgradeTenantDescriptors())
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					ID:        5,
					DataState: mtinfopb.DataStateAdd,
				},
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_ADD,
				}},
			}, manifest.Tenants)
	})
	t.Run("copies ProtoInfo fields to SQLInfo with state READY", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_READY,
				}},
			},
		}
		require.NoError(t, manifest.UpgradeTenantDescriptors())
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					ID:        5,
					DataState: mtinfopb.DataStateReady,
				},
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_READY,
				}},
			}, manifest.Tenants)
	})
	t.Run("copies ProtoInfo fields to SQLInfo with state DROP", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_DROP,
				}},
			},
		}
		require.NoError(t, manifest.UpgradeTenantDescriptors())
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					ID:        5,
					DataState: mtinfopb.DataStateDrop,
				},
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_DROP,
				}},
			}, manifest.Tenants)
	})
	t.Run("copies ProtoInfo fields to SQLInfo from deprecated tenants", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			TenantsDeprecated: []mtinfopb.ProtoInfo{{
				DeprecatedID:        5,
				DeprecatedDataState: mtinfopb.ProtoInfo_ADD,
			}},
		}
		require.NoError(t, manifest.UpgradeTenantDescriptors())
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					ID:        5,
					DataState: mtinfopb.DataStateAdd,
				},
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        5,
					DeprecatedDataState: mtinfopb.ProtoInfo_ADD,
				}},
			}, manifest.Tenants)
	})

	t.Run("returns error on partial SQLInfo", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					DataState: mtinfopb.DataStateReady,
				}},
			},
		}
		require.Error(t, manifest.UpgradeTenantDescriptors())
	})
	t.Run("return error on invalid data state", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				ProtoInfo: mtinfopb.ProtoInfo{
					DeprecatedID:        6,
					DeprecatedDataState: mtinfopb.ProtoInfo_DeprecatedDataState(99),
				}},
			},
		}
		require.Error(t, manifest.UpgradeTenantDescriptors())
	})
}
