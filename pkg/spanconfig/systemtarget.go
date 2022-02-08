// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

type SystemTarget struct {
	SourceTenantID roachpb.TenantID
	TargetTenantID roachpb.TenantID
}

func MakeSystemTarget(
	sourceTenantID roachpb.TenantID, targetTenantID roachpb.TenantID,
) (SystemTarget, error) {
	t := SystemTarget{
		SourceTenantID: sourceTenantID,
		TargetTenantID: targetTenantID,
	}
	return t, t.validate()
}

// encode implements the spanconfig.Target interface.
func (st SystemTarget) encode() roachpb.Span {
	var k roachpb.Key

	if st.SourceTenantID == roachpb.SystemTenantID &&
		st.TargetTenantID == roachpb.SystemTenantID {
		k = keys.SystemSpanConfigEntireKeyspace
	} else if st.SourceTenantID == roachpb.SystemTenantID {
		k = encoding.EncodeUvarintAscending(
			keys.SystemSpanConfigHostOnTenantKeyspace, st.TargetTenantID.InternalValue,
		)
	} else {
		k = encoding.EncodeUvarintAscending(
			keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace, st.SourceTenantID.InternalValue,
		)
	}
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

func (st SystemTarget) validate() error {
	if st.SourceTenantID != roachpb.SystemTenantID && st.SourceTenantID != st.TargetTenantID {
		return errors.AssertionFailedf(
			"secondary tenant %s cannot target another tenant with ID %s",
			st.SourceTenantID,
			st.TargetTenantID,
		)
	}
	return nil
}

// isEmpty returns true if the receiver is empty.
func (st SystemTarget) isEmpty() bool {
	return st.TargetTenantID.Equal(roachpb.TenantID{}) &&
		st.SourceTenantID.Equal(roachpb.TenantID{})
}

// String returns a pretty printed version of a system target.
func (st SystemTarget) String() string {
	return fmt.Sprintf("{system target source: %s target: %s}", st.SourceTenantID, st.TargetTenantID)
}

// decodeSystemTarget converts the given span into a SystemTarget. An error is
// returned if the supplied span does not conform to a  system target's
// encoding.
func decodeSystemTarget(span roachpb.Span) (SystemTarget, error) {
	switch {
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigEntireKeyspace):
		return MakeSystemTarget(roachpb.SystemTenantID, roachpb.SystemTenantID)
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace):
		// System span config was applied by the host tenant over a secondary
		// tenant's entire keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigHostOnTenantKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeSystemTarget(roachpb.SystemTenantID, tenID)
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):
		// System span config was applied by a secondary tenant over its entire
		// keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeSystemTarget(tenID, tenID)
	default:
		return SystemTarget{},
			errors.AssertionFailedf("span %s did not conform to SystemTarget encoding", span)
	}
}

// conformsToSystemTargetEncoding returns true if the given span conforms to the key
// encoding of system span configurations.
func conformsToSystemTargetEncoding(span roachpb.Span) bool {
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigEntireKeyspace) ||
		bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace) ||
		bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace) {
		return true
	}
	return false
}
