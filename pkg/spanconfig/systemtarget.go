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

// SystemTarget specifies the target of a system span configuration.
type SystemTarget struct {
	// SourceTenantID is the ID of the tenant that specified the system span
	// configuration.
	// SourceTenantID is the ID of the tenant that specified the system span
	// configuration.
	SourceTenantID roachpb.TenantID

	// TargetTenantID is the ID of the tenant that the associated system span
	// configuration applies to.
	//
	// If the host tenant is the source and the TargetTenantID is unspecified then
	// the associated system span configuration applies over all ranges in the
	// system (including those belonging to secondary tenants).
	//
	// Secondary tenants are only allowed to target themselves. The host tenant
	// may use this field to target a specific secondary tenant. We validate this
	// when constructing new system targets.
	TargetTenantID *roachpb.TenantID
}

// MakeSystemTarget constructs, validates, and returns a new SystemTarget.
func MakeSystemTarget(
	sourceTenantID roachpb.TenantID, targetTenantID *roachpb.TenantID,
) (SystemTarget, error) {
	t := SystemTarget{
		SourceTenantID: sourceTenantID,
		TargetTenantID: targetTenantID,
	}
	return t, t.validate()
}

// targetsEntireCluster returns true if the target applies to all ranges in the
// system (including those belonging to secondary tenants).
func (st SystemTarget) targetsEntireCluster() bool {
	return st.SourceTenantID == roachpb.SystemTenantID && st.TargetTenantID == nil
}

// encode returns an encoded span associated with the receiver which is suitable
// for persistence in system.span_configurations.
func (st SystemTarget) encode() roachpb.Span {
	var k roachpb.Key

	if st.SourceTenantID == roachpb.SystemTenantID &&
		st.TargetTenantID == nil {
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

// validate ensures that the receiver is well-formed.
func (st SystemTarget) validate() error {
	if st.SourceTenantID == roachpb.SystemTenantID {
		// The system tenant can target itself, other secondary tenants, and the
		// entire cluster (including secondary tenant ranges).
		return nil
	}
	if st.TargetTenantID == nil {
		return errors.AssertionFailedf(
			"secondary tenant %s cannot have unspecified target tenant ID", st.SourceTenantID,
		)
	}
	if st.SourceTenantID != *st.TargetTenantID {
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
	return st.SourceTenantID.Equal(roachpb.TenantID{}) && st.TargetTenantID.Equal(nil)
}

// less returns true if the receiver is considered less than the supplied
// target. The semantics are defined as follows:
// - host installed targets that target the entire cluster come first.
// - host installed targets that target a tenant come next (ordered by target
// tenant ID).
// - secondary tenant installed targets come next, ordered by secondary tenant
// ID.
func (st SystemTarget) less(ot SystemTarget) bool {
	if st.SourceTenantID == roachpb.SystemTenantID &&
		ot.SourceTenantID == roachpb.SystemTenantID {
		if st.targetsEntireCluster() {
			return true
		} else if ot.targetsEntireCluster() {
			return false
		}

		return st.TargetTenantID.InternalValue < ot.TargetTenantID.InternalValue
	}

	if st.SourceTenantID == roachpb.SystemTenantID {
		return true
	} else if ot.SourceTenantID == roachpb.SystemTenantID {
		return false
	}

	return st.SourceTenantID.InternalValue < ot.SourceTenantID.InternalValue
}

// equal returns true iff the receiver is equal to the supplied system target.
func (st SystemTarget) equal(ot SystemTarget) bool {
	return st.SourceTenantID == ot.SourceTenantID && st.TargetTenantID == ot.TargetTenantID
}

// String returns a pretty printed version of a system target.
func (st SystemTarget) String() string {
	return fmt.Sprintf("{system target source: %s target: %s}", st.SourceTenantID, st.TargetTenantID)
}

// decodeSystemTarget converts the given span into a SystemTarget. An error is
// returned if the supplied span does not conform to a  system target's
// encoding.
func decodeSystemTarget(span roachpb.Span) (SystemTarget, error) {
	// Validate the end key is well-formed.
	if !span.EndKey.Equal(span.Key.PrefixEnd()) {
		return SystemTarget{}, errors.AssertionFailedf("invalid end key in span %s", span)
	}
	switch {
	case bytes.Equal(span.Key, keys.SystemSpanConfigEntireKeyspace):
		return MakeSystemTarget(roachpb.SystemTenantID, nil /* targetTenantID */)
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace):
		// System span config was applied by the host tenant over a secondary
		// tenant's entire keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigHostOnTenantKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeSystemTarget(roachpb.SystemTenantID, &tenID)
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):
		// System span config was applied by a secondary tenant over its entire
		// keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeSystemTarget(tenID, &tenID)
	default:
		return SystemTarget{},
			errors.AssertionFailedf("span %s did not conform to SystemTarget encoding", span)
	}
}

// spanStartKeyConformsToSystemTargetEncoding returns true if the given span's
// start key conforms to the key encoding of system span configurations.
func spanStartKeyConformsToSystemTargetEncoding(span roachpb.Span) bool {
	return bytes.Equal(span.Key, keys.SystemSpanConfigEntireKeyspace) ||
		bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace) ||
		bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace)
}
