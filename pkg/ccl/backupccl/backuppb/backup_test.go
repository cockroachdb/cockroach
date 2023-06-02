// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuppb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/stretchr/testify/require"
)

func TestGetTenants(t *testing.T) {
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
		tenants, err := manifest.GetTenants()
		require.NoError(t, err)
		require.Equal(t,
			[]mtinfopb.TenantInfoWithUsage{
				{SQLInfo: mtinfopb.SQLInfo{ID: 5}},
			}, tenants)

	})
	t.Run("returns DeprecatedTenants when Tenants is empty", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			TenantsDeprecated: []mtinfopb.ProtoInfo{{
				DeprecatedID:        6,
				DeprecatedDataState: mtinfopb.ProtoInfo_READY,
			}},
		}
		tenants, err := manifest.GetTenants()
		require.NoError(t, err)
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
			}, tenants)

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
		tenants, err := manifest.GetTenants()
		require.NoError(t, err)
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
			}, tenants)
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
		tenants, err := manifest.GetTenants()
		require.NoError(t, err)
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
			}, tenants)
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
		tenants, err := manifest.GetTenants()
		require.NoError(t, err)
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
			}, tenants)
	})
	t.Run("copies ProtoInfo fields to SQLInfo from deprecated tenants", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			TenantsDeprecated: []mtinfopb.ProtoInfo{{
				DeprecatedID:        5,
				DeprecatedDataState: mtinfopb.ProtoInfo_ADD,
			}},
		}
		tenants, err := manifest.GetTenants()
		require.NoError(t, err)
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
			}, tenants)
	})

	t.Run("returns error on partial SQLInfo", func(t *testing.T) {
		manifest := backuppb.BackupManifest{
			Tenants: []mtinfopb.TenantInfoWithUsage{{
				SQLInfo: mtinfopb.SQLInfo{
					DataState: mtinfopb.DataStateReady,
				}},
			},
		}
		_, err := manifest.GetTenants()
		require.Error(t, err)
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
		_, err := manifest.GetTenants()
		require.Error(t, err)
	})
}
