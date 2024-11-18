// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfo"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// InitialValuesOpts is used to get initial values for system/secondary tenants
// and allows overriding initial values with ones from previous releases.
type InitialValuesOpts struct {
	DefaultZoneConfig       *zonepb.ZoneConfig
	DefaultSystemZoneConfig *zonepb.ZoneConfig
	OverrideKey             clusterversion.Key
	Codec                   keys.SQLCodec
	Txn                     *kv.Txn
	IsqlTxn                 isql.Txn
}

// GenerateInitialValues generates the initial values with which to bootstrap a
// new cluster. This generates the values assuming a cluster version equal to
// the latest binary, unless explicitly overridden.
func (opts InitialValuesOpts) GenerateInitialValues() ([]roachpb.KeyValue, []roachpb.RKey, error) {
	versionKey := clusterversion.Latest
	if opts.OverrideKey != 0 {
		versionKey = opts.OverrideKey
	}
	// TODO(shubham): deal with other `versionKey` later
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
	clusterversion.V24_3: hardCodedInitialValues{
		system:        v24_3_system_keys,
		systemHash:    v24_3_system_sha256,
		nonSystem:     v24_3_tenant_keys,
		nonSystemHash: v24_3_tenant_sha256,
	}.build,
	clusterversion.V24_2: hardCodedInitialValues{
		system:        v24_2_system_keys,
		systemHash:    v24_2_system_sha256,
		nonSystem:     v24_2_tenant_keys,
		nonSystemHash: v24_2_tenant_sha256,
	}.build,
	clusterversion.V24_1: hardCodedInitialValues{
		system:        v24_1_system_keys,
		systemHash:    v24_1_system_sha256,
		nonSystem:     v24_1_tenant_keys,
		nonSystemHash: v24_1_tenant_sha256,
	}.build,
}

// buildLatestInitialValues is the default initial value factory.
func buildLatestInitialValues(
	opts InitialValuesOpts,
) (kvs []roachpb.KeyValue, splits []roachpb.RKey, _ error) {
	schema := MakeMetadataSchema(opts.Codec, opts.DefaultZoneConfig, opts.DefaultSystemZoneConfig)
	kvs, splits = schema.GetInitialValues()

	if !roachpb.EnableExperimentalUA && opts.Codec.TenantID == roachpb.TenantTwo {
		copiedKVs, err := copySystemTableKVs(context.Background(), opts.Txn, opts.IsqlTxn, opts.Codec, schema.descsMap)
		if err != nil {
			return nil, nil, err
		}
		kvs = append(kvs, copiedKVs...)
	}

	return kvs, splits, nil
}

func copySystemTableKVs(
	ctx context.Context,
	kvTxn *kv.Txn,
	isqlTxn isql.Txn,
	targetCodec keys.SQLCodec,
	descMap map[string]catalog.Descriptor,
) ([]roachpb.KeyValue, error) {
	ret := make([]roachpb.KeyValue, 0)
	sourceCodec := keys.SystemSQLCodec

	if kvs, err := systemTenantTableKVs(
		ctx, kvTxn, isqlTxn, targetCodec, descMap); err != nil {
		return nil, err
	} else {
		ret = append(ret, kvs...)
	}

	tables := []catconstants.SystemTableName{
		catconstants.ZonesTableName, // we need to filter out data for tables we don't need
		catconstants.TenantSettingsTableName,
		// Latest value for `tenant_id_seq` can be read using `.Scan` because in
		// `updateTenantIDSequence` we use `.Put` to update instead of
		// `InternalExecutor`.
		catconstants.TenantIDSequenceTableName,
		catconstants.RoleIDSequenceName,
	}

	batch := kvTxn.NewBatch()
	for _, table := range tables {
		desc, ok := descMap[string(table)]
		if !ok {
			log.Ops.Errorf(ctx, "descID not found for : %s", table)
			return nil, errors.Errorf("descID not found for : %s", table)
		}
		span := sourceCodec.TableSpan(uint32(desc.GetID()))
		batch.Scan(span.Key, span.EndKey)
	}

	if err := kvTxn.Run(ctx, batch); err != nil {
		return nil, err
	}

	if len(batch.Results) != len(tables) {
		return nil, errors.AssertionFailedf(
			"unexpected batch result count, expected: %d, found: %d",
			len(tables),
			len(batch.Results))
	}

	for i, result := range batch.Results {
		if err := result.Err; err != nil {
			return nil, err
		}
		rows := result.Rows
		// Rewrite the keys
		tablePrefix := targetCodec.TablePrefix(uint32(descMap[string(tables[i])].GetID()))
		kvs := make([]roachpb.KeyValue, 0, len(rows))
		for _, row := range rows {
			strippedKey, err := keys.StripTablePrefix(row.Key)
			if err != nil {
				return nil, err
			}
			key := make([]byte, 0, len(tablePrefix)+len(strippedKey))
			key = append(key, tablePrefix...)
			key = append(key, strippedKey...)
			kv := roachpb.KeyValue{
				Key:   key,
				Value: *row.Value,
			}

			kv.Value.ClearChecksum()
			kvs = append(kvs, kv)
		}
		ret = append(ret, kvs...)
	}

	return ret, nil
}

func systemTenantTableKVs(
	ctx context.Context,
	_ *kv.Txn,
	isqlTxn isql.Txn,
	targetCodec keys.SQLCodec,
	descMap map[string]catalog.Descriptor,
) ([]roachpb.KeyValue, error) {
	ret := make([]roachpb.KeyValue, 0)

	tenants, err := GetTenantRecords(ctx, isqlTxn)
	const expectedTenantsCount = 2
	if err != nil {
		return nil, err
	} else if len(tenants) < expectedTenantsCount {
		return nil, errors.Errorf(
			"unexpected tenants count: expected %d, found %d",
			expectedTenantsCount, len(tenants))
	}

	tenantOne := tenants[0]
	if tenantOne.ID != roachpb.TenantOne.ToUint64() {
		return nil, errors.Errorf(
			"expected first tenant with id %d", roachpb.TenantOne.ToUint64())
	}
	tenantTwo := tenants[1]
	if tenantTwo.ID != roachpb.TenantTwo.ToUint64() {
		return nil, errors.Errorf(
			"expected second tenant with id %d", roachpb.TenantTwo.ToUint64())
	}

	tenantOne.Name = tenantTwo.Name
	tenantTwo.Name = catconstants.SystemTenantName

	desc := descMap[string(catconstants.TenantsTableName)]
	tenantsTableWriter := MakeKVWriter(targetCodec, desc.(catalog.TableDescriptor))
	for _, tenantInfo := range tenants {
		info := tenantInfo.ProtoInfo
		infoBytes, err := protoutil.Marshal(&info)
		if err != nil {
			panic(err)
		}
		kvs, err := tenantsTableWriter.RecordToKeyValues(
			// ID
			tree.NewDInt(tree.DInt(tenantInfo.ID)),
			// active -- deprecated.
			tree.MakeDBool(true),
			// info.
			// TODO(shubham): Deal with some extra data later
			tree.NewDBytes(tree.DBytes(infoBytes)),
			// name.
			tree.NewDString(string(tenantInfo.Name)),
			// data_state.
			tree.NewDInt(tree.DInt(tenantInfo.DataState)),
			// service_mode.
			tree.NewDInt(tree.DInt(tenantInfo.ServiceMode)),
		)
		if err != nil {
			panic(err)
		}

		ret = append(ret, kvs...)
	}

	return ret, nil
}

// GetTenantRecords
//
// TODO(shubham): use sql.GetTenantRecordByID instead. It's copied as it is to
// avoid some dependency headache for the time being. GetTenantRecordByID
// retrieves a tenant in system.tenants.
func GetTenantRecords(ctx context.Context, txn isql.Txn) ([]*mtinfopb.TenantInfo, error) {
	q := `SELECT id, info, name, data_state, service_mode FROM system.tenants ORDER BY id`
	var arg interface{} = mtinfopb.DataStateDrop

	rows, err := txn.QueryBufferedEx(ctx, "get-tenants-info",
		txn.KV(), sessiondata.NodeUserSessionDataOverride, q, arg)
	if err != nil {
		return nil, err
	}

	tenants := make([]*mtinfopb.TenantInfo, 0, len(rows))
	for _, row := range rows {
		_, info, err := mtinfo.GetTenantInfoFromSQLRow(row)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, info)
	}

	return tenants, nil
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

//go:embed data/24_1_system.keys
var v24_1_system_keys string

//go:embed data/24_1_system.sha256
var v24_1_system_sha256 string

//go:embed data/24_1_tenant.keys
var v24_1_tenant_keys string

//go:embed data/24_1_tenant.sha256
var v24_1_tenant_sha256 string

//go:embed data/24_2_system.keys
var v24_2_system_keys string

//go:embed data/24_2_system.sha256
var v24_2_system_sha256 string

//go:embed data/24_2_tenant.keys
var v24_2_tenant_keys string

//go:embed data/24_2_tenant.sha256
var v24_2_tenant_sha256 string

//go:embed data/24_3_system.keys
var v24_3_system_keys string

//go:embed data/24_3_system.sha256
var v24_3_system_sha256 string

//go:embed data/24_3_tenant.keys
var v24_3_tenant_keys string

//go:embed data/24_3_tenant.sha256
var v24_3_tenant_sha256 string
