// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bootstrap

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// InitialValuesOpts is used to get initial values for system/secondary tenants
// and allows overriding initial values with ones from previous releases.
type InitialValuesOpts struct {
	DefaultZoneConfig       *zonepb.ZoneConfig
	DefaultSystemZoneConfig *zonepb.ZoneConfig
	OverrideKey             clusterversion.Key
	Codec                   keys.SQLCodec
}

// GenerateInitialValues generates the initial values with which to bootstrap a
// new cluster. This generates the values assuming a cluster version equal to
// the latest binary, unless explicitly overridden.
func (opts InitialValuesOpts) GenerateInitialValues() ([]roachpb.KeyValue, []roachpb.RKey, error) {
	versionKey := clusterversion.Latest
	if opts.OverrideKey != 0 {
		versionKey = opts.OverrideKey
	}
	f, ok := initialValuesFactoryByKey[versionKey]
	if !ok {
		return nil, nil, errors.Newf("unsupported version %q", versionKey)
	}
	return f(opts)
}

// VersionsWithInitialValues returns all the versions which can be used for
// InitialValuesOpts.OverrideKey, in descending order; the first one is always
// the current version.
func VersionsWithInitialValues() []clusterversion.Key {
	res := make([]clusterversion.Key, 0, len(initialValuesFactoryByKey))
	for k := range initialValuesFactoryByKey {
		res = append(res, k)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i] > res[j]
	})
	return res
}

type initialValuesFactoryFn = func(opts InitialValuesOpts) (
	kvs []roachpb.KeyValue, splits []roachpb.RKey, _ error,
)

var initialValuesFactoryByKey = map[clusterversion.Key]initialValuesFactoryFn{
	clusterversion.Latest: buildLatestInitialValues,

	clusterversion.V25_1: hardCodedInitialValues{
		system:        v25_1_system_keys,
		systemHash:    v25_1_system_sha256,
		nonSystem:     v25_1_tenant_keys,
		nonSystemHash: v25_1_tenant_sha256,
	}.build,

	clusterversion.V24_3: hardCodedInitialValues{
		system:        v24_3_system_keys,
		systemHash:    v24_3_system_sha256,
		nonSystem:     v24_3_tenant_keys,
		nonSystemHash: v24_3_tenant_sha256,
	}.build,
}

// buildLatestInitialValues is the default initial value factory.
func buildLatestInitialValues(
	opts InitialValuesOpts,
) (kvs []roachpb.KeyValue, splits []roachpb.RKey, _ error) {
	schema := MakeMetadataSchema(opts.Codec, opts.DefaultZoneConfig, opts.DefaultSystemZoneConfig)
	kvs, splits = schema.GetInitialValues()
	return kvs, splits, nil
}

// hardCodedInitialValues defines an initialValuesFactoryFn using
// hard-coded values.
type hardCodedInitialValues struct {
	system, systemHash, nonSystem, nonSystemHash string
}

// build implements initialValuesFactoryFn for hardCodedInitialValues.
func (f hardCodedInitialValues) build(
	opts InitialValuesOpts,
) (kvs []roachpb.KeyValue, splits []roachpb.RKey, err error) {
	defer func() {
		err = errors.Wrapf(err, "error making initial values for tenant %s", opts.Codec.TenantPrefix())
	}()
	input, expectedHash := f.system, f.systemHash
	if !opts.Codec.ForSystemTenant() {
		input, expectedHash = f.nonSystem, f.nonSystemHash
	}
	input = strings.TrimSpace(input)
	expectedHash = strings.TrimSpace(expectedHash)
	h := sha256.Sum256([]byte(input))
	if actualHash := hex.EncodeToString(h[:]); actualHash != expectedHash {
		return nil, nil, errors.AssertionFailedf("expected %s, got %s", expectedHash, actualHash)
	}
	var initialKVs []roachpb.KeyValue
	initialKVs, splits, err = InitialValuesFromString(opts.Codec, input)
	if err != nil {
		return nil, nil, err
	}
	// Replace system.zones entries.
	kvs = InitialZoneConfigKVs(opts.Codec, opts.DefaultZoneConfig, opts.DefaultSystemZoneConfig)
	zonesTablePrefix := opts.Codec.TablePrefix(keys.ZonesTableID)
	for _, kv := range initialKVs {
		if !bytes.Equal(zonesTablePrefix, kv.Key[:len(zonesTablePrefix)]) {
			kvs = append(kvs, kv)
		}
	}
	sort.Sort(roachpb.KeyValueByKey(kvs))
	return kvs, splits, nil
}

// The following variables hold hardcoded bootstrap data for older versions (as
// produced by the final release version). For each version, we have system and
// non-system keys, and each set of keys has an associated SHA-256.
//
// These files can be auto-generated for the latest version with the
// sql-bootstrap-data CLI tool (see pkg/cmd/sql-bootstrap-data).

//go:embed data/24_3_system.keys
var v24_3_system_keys string

//go:embed data/24_3_system.sha256
var v24_3_system_sha256 string

//go:embed data/24_3_tenant.keys
var v24_3_tenant_keys string

//go:embed data/24_3_tenant.sha256
var v24_3_tenant_sha256 string

//go:embed data/25_1_system.keys
var v25_1_system_keys string

//go:embed data/25_1_system.sha256
var v25_1_system_sha256 string

//go:embed data/25_1_tenant.keys
var v25_1_tenant_keys string

//go:embed data/25_1_tenant.sha256
var v25_1_tenant_sha256 string
