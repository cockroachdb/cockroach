// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mtinfo

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type ReadFromTenantInfoAccessor interface {
	ReadFromTenantInfo(context.Context) (roachpb.TenantID, hlc.Timestamp, error)
}

// GetTenantInfoFromSQLRow synthetizes a TenantInfo from a SQL row
// extracted from system.tenants. The caller is responsible for
// passing a []tree.Datum of length 2 or more (ID, info, name*, data
// state*, service mode*). The values besides ID and info can be
// absent or NULL; the logic knows how to handle data collected across
// the v23.1 version boundary that introduced the name, data state and
// service mode columns.
func GetTenantInfoFromSQLRow(
	row tree.Datums,
) (tid roachpb.TenantID, info *mtinfopb.TenantInfo, err error) {
	if len(row) < 2 {
		return tid, nil, errors.AssertionFailedf("expected at least 2 columns, got %d", len(row))
	}
	info = &mtinfopb.TenantInfo{}

	idval, ok := tree.AsDInt(row[0])
	if !ok {
		return tid, nil, errors.AssertionFailedf("tenant ID: expected int, got %T", row[0])
	}
	tid, err = roachpb.MakeTenantID(uint64(idval))
	if err != nil {
		return tid, nil, errors.NewAssertionErrorWithWrappedErrf(err, "%v", idval)
	}

	info.ID = tid.ToUint64()

	// For the benefit of pre-23.1 BACKUP/RESTORE.
	info.DeprecatedID = info.ID

	if len(row) < 2 || row[1] == tree.DNull {
		return tid, nil, errors.AssertionFailedf("%v: missing data in info column", tid)
	}
	ival, ok := tree.AsDBytes(row[1])
	if !ok {
		return tid, nil, errors.AssertionFailedf("%v: info: expected bytes, got %T", tid, row[1])
	}
	infoBytes := []byte(ival)
	if err := protoutil.Unmarshal(infoBytes, &info.ProtoInfo); err != nil {
		return tid, nil, errors.NewAssertionErrorWithWrappedErrf(err, "%v: decoding info column", tid)
	}

	// If we are loading the entry for the system tenant, inject all the
	// capabilities that the system tenant should have. This will allow
	// us to reduce our dependency on a tenant ID check to retain the
	// system tenant's access to all services.
	if tid.IsSystem() {
		tenantcapabilities.EnableAll(&info.ProtoInfo.Capabilities)
	}

	// Load the name if defined.
	if len(row) > 2 && row[2] != tree.DNull {
		name, ok := tree.AsDString(row[2])
		if !ok {
			return tid, nil, errors.AssertionFailedf("%v: name: expected string, got %T", tid, row[2])
		}
		info.Name = roachpb.TenantName(name)
	}

	// Load the data state column if defined.
	// Compute a suitable default value, from the pre-v23.1 info struct.
	switch info.ProtoInfo.DeprecatedDataState {
	case mtinfopb.ProtoInfo_READY:
		info.DataState = mtinfopb.DataStateReady
	case mtinfopb.ProtoInfo_ADD:
		info.DataState = mtinfopb.DataStateAdd
	case mtinfopb.ProtoInfo_DROP:
		info.DataState = mtinfopb.DataStateDrop
	default:
		return tid, nil, errors.AssertionFailedf("%v: unhandled: %d", tid, info.ProtoInfo.DeprecatedDataState)
	}
	if len(row) > 3 && row[3] != tree.DNull {
		val, ok := tree.AsDInt(row[3])
		if !ok {
			return tid, nil, errors.AssertionFailedf("%v: data state: expected int, got %T", tid, row[3])
		}
		if val < 0 || mtinfopb.TenantDataState(val) > mtinfopb.MaxDataState {
			return tid, nil, errors.AssertionFailedf("%v: invalid data state: %d", tid, val)
		} else {
			info.DataState = mtinfopb.TenantDataState(val)
		}
	}

	// Load the service mode if defined.
	if info.DataState == mtinfopb.DataStateReady {
		// Suitable default for records created for CC Serverless pre-v23.1.
		info.ServiceMode = mtinfopb.ServiceModeExternal
	} else {
		// Suitable default for in-progress records created for CC Serverless pre-v23.1.
		info.ServiceMode = mtinfopb.ServiceModeNone
	}
	if len(row) > 4 && row[4] != tree.DNull {
		val, ok := tree.AsDInt(row[4])
		if !ok {
			return tid, nil, errors.AssertionFailedf("%v: service mode: expected int, got %T", tid, row[4])
		}
		if val < 0 || mtinfopb.TenantServiceMode(val) > mtinfopb.MaxServiceMode {
			return tid, nil, errors.AssertionFailedf("%v: invalid service mode: %d", tid, val)
		} else {
			info.ServiceMode = mtinfopb.TenantServiceMode(val)
		}
	}

	return tid, info, nil
}
