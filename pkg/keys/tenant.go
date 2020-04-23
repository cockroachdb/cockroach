// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// MakeTenantPrefix creates the key prefix associated with the specified tenant.
func MakeTenantPrefix(tenID roachpb.TenantID) roachpb.Key {
	switch tenID {
	case 0: // protect against zero value
		panic("invalid tenant ID 0")
	case roachpb.SystemTenantID:
		return nil
	default:
		return encoding.EncodeUvarintAscending(tenantPrefix, uint64(tenID))
	}
}

// DecodeTenantPrefix determines the tenant ID from the key prefix, returning
// the remainder of the key (with the prefix removed) and the decoded tenant ID.
func DecodeTenantPrefix(key roachpb.Key) ([]byte, roachpb.TenantID, error) {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return nil, roachpb.SystemTenantID, nil
	}
	if key[0] != tenantPrefixByte {
		return key, roachpb.SystemTenantID, nil
	}
	rem, tenID, err := encoding.DecodeUvarintAscending(key[1:])
	return rem, roachpb.TenantID(tenID), err
}
