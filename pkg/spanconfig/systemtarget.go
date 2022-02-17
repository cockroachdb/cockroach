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
	// sourceTenantID is the ID of the tenant that specified the system span
	// configuration.
	sourceTenantID roachpb.TenantID

	// targetTenantID is the ID of the tenant that the associated system span
	// configuration applies. This field can only be set in conjunction with the
	// type being SystemTargetTypeSpecificTenant; it must be left unset for all
	// other system target types.
	//
	// Secondary tenants are only allowed to target themselves. The host tenant
	// may use this field to target a specific secondary tenant.
	targetTenantID *roachpb.TenantID

	// systemTargetType indicates the type of the system target. targetTenantID
	// can only be set if the system target is specific.
	systemTargetType systemTargetType
}

// systemTargetType indicates the type of SystemTarget.
type systemTargetType int

const (
	_ systemTargetType = iota
	// SystemTargetTypeSpecificTenant indicates that the system target is
	// targeting a specific tenant.
	SystemTargetTypeSpecificTenant
	// SystemTargetTypeEntireCluster indicates that the system target is targeting
	// the entire cluster, i.e, all ranges in the system. Only the host tenant is
	// allowed to do so.
	SystemTargetTypeEntireCluster
	// SystemTargetTypeEverythingTargetingTenants represents all system span
	// configurations installed by a source tenant that target specific tenants.
	// This is a read-only system target.
	SystemTargetTypeEverythingTargetingTenants
)

// MakeTenantTarget constructs, validates, and returns a new SystemTarget that
// targets the physical keyspace of the targetTenantID.
func MakeTenantTarget(
	sourceTenantID roachpb.TenantID, targetTenantID roachpb.TenantID,
) (SystemTarget, error) {
	t := SystemTarget{
		sourceTenantID:   sourceTenantID,
		targetTenantID:   &targetTenantID,
		systemTargetType: SystemTargetTypeSpecificTenant,
	}
	return t, t.validate()
}

// MakeSystemTargetFromProto constructs a SystemTarget from a
// roachpb.SystemSpanConfigTarget and validates it.
func MakeSystemTargetFromProto(proto *roachpb.SystemSpanConfigTarget) (SystemTarget, error) {
	var t SystemTarget
	switch proto.SystemTargetType {
	case roachpb.SystemSpanConfigTarget_SpecificTenant:
		t = SystemTarget{
			sourceTenantID:   proto.SourceTenantID,
			targetTenantID:   proto.TargetTenantID,
			systemTargetType: SystemTargetTypeSpecificTenant,
		}
	case roachpb.SystemSpanConfigTarget_EntireCluster:
		t = SystemTarget{
			sourceTenantID:   proto.SourceTenantID,
			targetTenantID:   proto.TargetTenantID,
			systemTargetType: SystemTargetTypeEntireCluster,
		}
	case roachpb.SystemSpanConfigTarget_EverythingTargetingTenants:
		t = SystemTarget{
			sourceTenantID:   proto.SourceTenantID,
			targetTenantID:   proto.TargetTenantID,
			systemTargetType: SystemTargetTypeEverythingTargetingTenants,
		}
	case roachpb.SystemSpanConfigTarget_Unset:
		return SystemTarget{}, errors.AssertionFailedf("system target type unset on proto")
	}
	return t, t.validate()
}

// MakeClusterTarget returns a new system target that targets the entire cluster.
// Only the host tenant is allowed to target the entire cluster.
func MakeClusterTarget() SystemTarget {
	return SystemTarget{
		sourceTenantID:   roachpb.SystemTenantID,
		systemTargetType: SystemTargetTypeEntireCluster,
	}
}

// MakeEverythingTargetingTenantsTarget returns a new SystemTarget that
// represents all system span configurations installed by the given tenant ID
// on specific tenants (including both itself and other tenants).
func MakeEverythingTargetingTenantsTarget(sourceID roachpb.TenantID) SystemTarget {
	return SystemTarget{
		sourceTenantID:   sourceID,
		systemTargetType: SystemTargetTypeEverythingTargetingTenants,
	}
}

// targetsEntireCluster returns true if the target applies to all ranges in the
// system (including those belonging to secondary tenants).
func (st SystemTarget) targetsEntireCluster() bool {
	return st.systemTargetType == SystemTargetTypeEntireCluster
}

// IsReadOnly returns true if the system target is read-only. Read only targets
// should not be persisted.
func (st SystemTarget) IsReadOnly() bool {
	return st.systemTargetType == SystemTargetTypeEverythingTargetingTenants
}

// encode returns an encoded span associated with the receiver which is suitable
// for interaction with system.span_configurations table.
func (st SystemTarget) encode() roachpb.Span {
	var k roachpb.Key

	switch st.systemTargetType {
	case SystemTargetTypeEntireCluster:
		k = keys.SystemSpanConfigEntireKeyspace
	case SystemTargetTypeSpecificTenant:
		if st.sourceTenantID == roachpb.SystemTenantID {
			k = encoding.EncodeUvarintAscending(
				keys.SystemSpanConfigHostOnTenantKeyspace, st.targetTenantID.ToUint64(),
			)
		} else {
			k = encoding.EncodeUvarintAscending(
				keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace, st.sourceTenantID.ToUint64(),
			)
		}
	case SystemTargetTypeEverythingTargetingTenants:
		if st.sourceTenantID == roachpb.SystemTenantID {
			k = keys.SystemSpanConfigHostOnTenantKeyspace
		} else {
			k = encoding.EncodeUvarintAscending(
				keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace, st.sourceTenantID.ToUint64(),
			)
		}
	}
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

// validate ensures that the receiver is well-formed.
func (st SystemTarget) validate() error {
	switch st.systemTargetType {
	case SystemTargetTypeEverythingTargetingTenants:
		if st.targetTenantID != nil {
			return errors.AssertionFailedf(
				"targetTenantID must be unset when targeting everything installed on tenants",
			)
		}
	case SystemTargetTypeEntireCluster:
		if st.sourceTenantID != roachpb.SystemTenantID {
			return errors.AssertionFailedf("only the host tenant is allowed to target the entire cluster")
		}
		if st.targetTenantID != nil {
			return errors.AssertionFailedf("malformed system target for entire cluster; targetTenantID set")
		}
	case SystemTargetTypeSpecificTenant:
		if st.targetTenantID == nil {
			return errors.AssertionFailedf("malformed system target for specific tenant; targetTenantID unset")
		}
		if st.sourceTenantID != roachpb.SystemTenantID && st.sourceTenantID != *st.targetTenantID {
			return errors.AssertionFailedf(
				"secondary tenant %s cannot target another tenant with ID %s",
				st.sourceTenantID,
				st.targetTenantID,
			)
		}
	default:
		return errors.AssertionFailedf("invalid system target type")
	}
	return nil
}

// isEmpty returns true if the receiver is empty.
func (st SystemTarget) isEmpty() bool {
	return st.sourceTenantID.Equal(roachpb.TenantID{}) && st.targetTenantID.Equal(nil)
}

// less returns true if the receiver is considered less than the supplied
// target. The semantics are defined as follows:
// - read only targets come first, ordered by tenant ID.
// - targets that target the entire cluster come next.
// - targets that target specific tenant come last, sorted by source tenant ID;
// target tenant ID is used as a tiebreaker if the source's are the same.
func (st SystemTarget) less(ot SystemTarget) bool {
	if st.IsReadOnly() && ot.IsReadOnly() {
		return st.sourceTenantID.ToUint64() < ot.sourceTenantID.ToUint64()
	}

	if st.IsReadOnly() {
		return true
	} else if ot.IsReadOnly() {
		return false
	}

	if st.targetsEntireCluster() {
		return true
	} else if ot.targetsEntireCluster() {
		return false
	}

	if st.sourceTenantID.ToUint64() == ot.sourceTenantID.ToUint64() {
		return st.targetTenantID.ToUint64() < ot.targetTenantID.ToUint64()
	}

	return st.sourceTenantID.ToUint64() < ot.sourceTenantID.ToUint64()
}

// equal returns true iff the receiver is equal to the supplied system target.
func (st SystemTarget) equal(ot SystemTarget) bool {
	return st.sourceTenantID.Equal(ot.sourceTenantID) && st.targetTenantID.Equal(ot.targetTenantID)
}

// String returns a pretty printed version of a system target.
func (st SystemTarget) String() string {
	switch st.systemTargetType {
	case SystemTargetTypeEntireCluster:
		return "{cluster}"
	case SystemTargetTypeEverythingTargetingTenants:
		return fmt.Sprintf("{source=%d, everything-installed-on-tenants}", st.sourceTenantID)
	case SystemTargetTypeSpecificTenant:
		return fmt.Sprintf(
			"{source=%d,target=%d}",
			st.sourceTenantID.ToUint64(),
			st.targetTenantID.ToUint64(),
		)
	default:
		panic("unreachable")
	}
}

// decodeSystemTarget converts the given span into a SystemTarget. An error is
// returned if the supplied span does not conform to a system target's encoding.
func decodeSystemTarget(span roachpb.Span) (SystemTarget, error) {
	// Validate the end key is well-formed.
	if !span.EndKey.Equal(span.Key.PrefixEnd()) {
		return SystemTarget{}, errors.AssertionFailedf("invalid end key in span %s", span)
	}
	switch {
	case bytes.Equal(span.Key, keys.SystemSpanConfigEntireKeyspace):
		return MakeClusterTarget(), nil
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace):
		// System span config was applied by the host tenant over a secondary
		// tenant's entire keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigHostOnTenantKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeTenantTarget(roachpb.SystemTenantID, tenID)
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):
		// System span config was applied by a secondary tenant over its entire
		// keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeTenantTarget(tenID, tenID)
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
