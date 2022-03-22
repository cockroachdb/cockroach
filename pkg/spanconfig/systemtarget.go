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

	// targetTenantID is the ID of the tenant whose kesypace the associated system
	// span configuration applies. This field can only be set in conjunction with
	// the type being SystemTargetTypeSpecificTenantKeyspace; it must be left
	// unset for all other system target types.
	//
	// Secondary tenants are only allowed to target their own keyspace. The host
	// tenant may use this field to target a specific secondary tenant.
	targetTenantID roachpb.TenantID

	// systemTargetType indicates the type of the system target. targetTenantID
	// can only be set if the system target is specific.
	systemTargetType systemTargetType
}

// systemTargetType indicates the type of SystemTarget.
type systemTargetType int

const (
	_ systemTargetType = iota
	// SystemTargetTypeSpecificTenantKeyspace indicates that the system target is
	// targeting a specific tenant's keyspace.
	SystemTargetTypeSpecificTenantKeyspace
	// SystemTargetTypeEntireKeyspace indicates that the system target is
	// targeting the entire keyspace. Only the host tenant is allowed to do so.
	SystemTargetTypeEntireKeyspace
	// SystemTargetTypeAllTenantKeyspaceTargetsSet represents a system target that
	// encompasses all system targets that have been set by the source tenant over
	// specific tenant's keyspace.
	//
	// This is a read-only system target type as it may translate to more than one
	// system targets that may have been persisted. This target type is useful in
	// fetching all system span configurations a tenant may have set on tenant
	// keyspaces without knowing the tenant ID of all other tenants in the system.
	// This is only ever significant for the host tenant as it can set system span
	// configurations that target other tenant's keyspaces.
	SystemTargetTypeAllTenantKeyspaceTargetsSet
)

// MakeTenantKeyspaceTarget constructs, validates, and returns a new
// SystemTarget that targets the keyspace of the target tenant.
func MakeTenantKeyspaceTarget(
	sourceTenantID roachpb.TenantID, targetTenantID roachpb.TenantID,
) (SystemTarget, error) {
	t := SystemTarget{
		sourceTenantID:   sourceTenantID,
		targetTenantID:   targetTenantID,
		systemTargetType: SystemTargetTypeSpecificTenantKeyspace,
	}
	return t, t.validate()
}

// makeSystemTargetFromProto constructs a SystemTarget from a
// roachpb.SystemSpanConfigTarget and validates it.
func makeSystemTargetFromProto(proto *roachpb.SystemSpanConfigTarget) (SystemTarget, error) {
	var t SystemTarget
	switch {
	case proto.IsSpecificTenantKeyspaceTarget():
		t = SystemTarget{
			sourceTenantID:   proto.SourceTenantID,
			targetTenantID:   proto.Type.GetSpecificTenantKeyspace().TenantID,
			systemTargetType: SystemTargetTypeSpecificTenantKeyspace,
		}
	case proto.IsEntireKeyspaceTarget():
		t = SystemTarget{
			sourceTenantID:   proto.SourceTenantID,
			targetTenantID:   roachpb.TenantID{},
			systemTargetType: SystemTargetTypeEntireKeyspace,
		}
	case proto.IsAllTenantKeyspaceTargetsSetTarget():
		t = SystemTarget{
			sourceTenantID:   proto.SourceTenantID,
			targetTenantID:   roachpb.TenantID{},
			systemTargetType: SystemTargetTypeAllTenantKeyspaceTargetsSet,
		}
	default:
		return SystemTarget{}, errors.AssertionFailedf("unknown system target type")
	}
	return t, t.validate()
}

func (st SystemTarget) toProto() *roachpb.SystemSpanConfigTarget {
	var systemTargetType *roachpb.SystemSpanConfigTarget_Type
	switch st.systemTargetType {
	case SystemTargetTypeEntireKeyspace:
		systemTargetType = roachpb.NewEntireKeyspaceTargetType()
	case SystemTargetTypeAllTenantKeyspaceTargetsSet:
		systemTargetType = roachpb.NewAllTenantKeyspaceTargetsSetTargetType()
	case SystemTargetTypeSpecificTenantKeyspace:
		systemTargetType = roachpb.NewSpecificTenantKeyspaceTargetType(st.targetTenantID)
	default:
		panic("unknown system target type")
	}
	return &roachpb.SystemSpanConfigTarget{
		SourceTenantID: st.sourceTenantID,
		Type:           systemTargetType,
	}
}

// MakeEntireKeyspaceTarget returns a new system target that targets the entire
// keyspace. Only the host tenant is allowed to target the entire keyspace.
func MakeEntireKeyspaceTarget() SystemTarget {
	return SystemTarget{
		sourceTenantID:   roachpb.SystemTenantID,
		systemTargetType: SystemTargetTypeEntireKeyspace,
	}
}

// MakeAllTenantKeyspaceTargetsSet returns a new SystemTarget that
// represents all system span configurations installed by the given tenant ID
// on specific tenant's keyspace (including itself and other tenants).
func MakeAllTenantKeyspaceTargetsSet(sourceID roachpb.TenantID) SystemTarget {
	return SystemTarget{
		sourceTenantID:   sourceID,
		systemTargetType: SystemTargetTypeAllTenantKeyspaceTargetsSet,
	}
}

// targetsEntireKeyspace returns true if the target applies to all ranges in the
// system (including those belonging to secondary tenants).
func (st SystemTarget) targetsEntireKeyspace() bool {
	return st.systemTargetType == SystemTargetTypeEntireKeyspace
}

// keyspaceTargeted returns the keyspan the system target applies to.
func (st SystemTarget) keyspaceTargeted() roachpb.Span {
	switch st.systemTargetType {
	case SystemTargetTypeEntireKeyspace:
		return keys.EverythingSpan
	case SystemTargetTypeSpecificTenantKeyspace:
		// If the system tenant's keyspace is being targeted then this means
		// everything from the start of the keyspace to where all non-system tenant
		// keys begin.
		if st.targetTenantID == roachpb.SystemTenantID {
			return roachpb.Span{
				Key:    keys.MinKey,
				EndKey: keys.TenantTableDataMin,
			}
		}
		k := keys.MakeTenantPrefix(st.targetTenantID)
		return roachpb.Span{
			Key:    k,
			EndKey: k.PrefixEnd(),
		}
	case SystemTargetTypeAllTenantKeyspaceTargetsSet:
		// AllTenantKeyspaceTarget encapsulates other target; by itself, it doesn't
		// target a single contiguous keyspace.
		panic("not applicable")
	default:
		panic("unknown target type")
	}
}

// IsReadOnly returns true if the system target is read-only. Read only targets
// should not be persisted.
func (st SystemTarget) IsReadOnly() bool {
	return st.systemTargetType == SystemTargetTypeAllTenantKeyspaceTargetsSet
}

// encode returns an encoded span associated with the receiver which is suitable
// for interaction with system.span_configurations table.
func (st SystemTarget) encode() roachpb.Span {
	var k roachpb.Key

	switch st.systemTargetType {
	case SystemTargetTypeEntireKeyspace:
		k = keys.SystemSpanConfigEntireKeyspace
	case SystemTargetTypeSpecificTenantKeyspace:
		if st.sourceTenantID == roachpb.SystemTenantID {
			k = encoding.EncodeUvarintAscending(
				keys.SystemSpanConfigHostOnTenantKeyspace, st.targetTenantID.ToUint64(),
			)
		} else {
			k = encoding.EncodeUvarintAscending(
				keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace, st.sourceTenantID.ToUint64(),
			)
		}
	case SystemTargetTypeAllTenantKeyspaceTargetsSet:
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
	case SystemTargetTypeAllTenantKeyspaceTargetsSet:
		if st.targetTenantID.IsSet() {
			return errors.AssertionFailedf(
				"targetTenantID must be unset when targeting everything installed on tenants",
			)
		}
	case SystemTargetTypeEntireKeyspace:
		if st.sourceTenantID != roachpb.SystemTenantID {
			return errors.AssertionFailedf("only the host tenant is allowed to target the entire keyspace")
		}
		if st.targetTenantID.IsSet() {
			return errors.AssertionFailedf("malformed system target for entire keyspace; targetTenantID set")
		}
	case SystemTargetTypeSpecificTenantKeyspace:
		if !st.targetTenantID.IsSet() {
			return errors.AssertionFailedf(
				"malformed system target for specific tenant keyspace; targetTenantID unset",
			)
		}
		if st.sourceTenantID != roachpb.SystemTenantID && st.sourceTenantID != st.targetTenantID {
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

// IsEmpty returns true if the receiver is empty.
func (st SystemTarget) IsEmpty() bool {
	return !st.sourceTenantID.IsSet() && !st.targetTenantID.IsSet() &&
		st.systemTargetType == 0 // unset
}

// less returns true if the receiver is considered less than the supplied
// target. The semantics are defined as follows:
// - read only targets come first, ordered by tenant ID.
// - targets that target the entire keyspace come next.
// - targets that target a specific tenant's keyspace come last, sorted by
// source tenant ID; target tenant ID is used as a tiebreaker if two targets
// have the same source.
func (st SystemTarget) less(ot SystemTarget) bool {
	if st.IsReadOnly() && ot.IsReadOnly() {
		return st.sourceTenantID.ToUint64() < ot.sourceTenantID.ToUint64()
	}

	if st.IsReadOnly() {
		return true
	} else if ot.IsReadOnly() {
		return false
	}

	if st.targetsEntireKeyspace() {
		return true
	} else if ot.targetsEntireKeyspace() {
		return false
	}

	if st.sourceTenantID.ToUint64() == ot.sourceTenantID.ToUint64() {
		return st.targetTenantID.ToUint64() < ot.targetTenantID.ToUint64()
	}

	return st.sourceTenantID.ToUint64() < ot.sourceTenantID.ToUint64()
}

// equal returns true iff the receiver is equal to the supplied system target.
func (st SystemTarget) equal(ot SystemTarget) bool {
	return st.sourceTenantID.Equal(ot.sourceTenantID) &&
		st.targetTenantID.Equal(ot.targetTenantID) &&
		st.systemTargetType == ot.systemTargetType
}

// String returns a pretty printed version of a system target.
func (st SystemTarget) String() string {
	switch st.systemTargetType {
	case SystemTargetTypeEntireKeyspace:
		return "{entire-keyspace}"
	case SystemTargetTypeAllTenantKeyspaceTargetsSet:
		return fmt.Sprintf("{source=%d, all-tenant-keyspace-targets-set}", st.sourceTenantID)
	case SystemTargetTypeSpecificTenantKeyspace:
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
		return MakeEntireKeyspaceTarget(), nil
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace):
		// System span config was applied by the host tenant over a secondary
		// tenant's entire keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigHostOnTenantKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeTenantKeyspaceTarget(roachpb.SystemTenantID, tenID)
	case bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):
		// System span config was applied by a secondary tenant over its entire
		// keyspace.
		tenIDBytes := span.Key[len(keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return SystemTarget{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeTenantKeyspaceTarget(tenID, tenID)
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
