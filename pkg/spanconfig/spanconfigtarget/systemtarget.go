// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtarget

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

type SystemTarget struct {
	roachpb.SystemSpanConfigTarget
}

var _ spanconfig.Target = &SystemTarget{}

func NewSystemTarget(
	sourceTenantID roachpb.TenantID, targetTenantID roachpb.TenantID,
) (*SystemTarget, error) {
	t := &SystemTarget{
		SystemSpanConfigTarget: roachpb.SystemSpanConfigTarget{
			SourceTenantID: sourceTenantID,
			TargetTenantID: targetTenantID,
		},
	}
	return t, t.validate()
}

// Encode implements the spanconfig.Target interface.
func (target *SystemTarget) Encode() roachpb.Span {
	var k roachpb.Key

	if target.SourceTenantID == roachpb.SystemTenantID &&
		target.TargetTenantID == roachpb.SystemTenantID {
		k = keys.SystemSpanConfigEntireKeyspace
	} else if target.SourceTenantID == roachpb.SystemTenantID {
		k = encoding.EncodeUvarintAscending(
			keys.SystemSpanConfigHostOnTenantKeyspace, target.TargetTenantID.InternalValue,
		)
	} else {
		k = encoding.EncodeUvarintAscending(
			keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace, target.SourceTenantID.InternalValue,
		)
	}
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

// IsSystemTarget implements the spanconfig.Target interface.
func (t *SystemTarget) IsSystemTarget() bool {
	return true
}

// IsSpanTarget implements the spanconfig.Target interface.
func (t *SystemTarget) IsSpanTarget() bool {
	return false
}

// Less implements the spanconfig.Target interface.
func (t *SystemTarget) Less(o spanconfig.Target) bool {
	if o.IsSpanTarget() {
		return false
	}

	// We're dealing with 2 system targets.
	systemTargetO := o.TargetProto().GetSystemSpanConfigTarget()

	// TODO(arul): write out.
	if t.SourceTenantID == roachpb.SystemTenantID &&
		systemTargetO.SourceTenantID == roachpb.SystemTenantID {
		return t.TargetTenantID.InternalValue < systemTargetO.TargetTenantID.InternalValue
	} else if t.SourceTenantID == roachpb.SystemTenantID {
		return true
	} else if systemTargetO.SourceTenantID == roachpb.SystemTenantID {
		return false
	}

	// Secondary tenant targeting its logical cluster; sort by the secondary
	// tenant ID.
	return t.SourceTenantID.InternalValue < systemTargetO.SourceTenantID.InternalValue

}

// Equal implements the spanconfig.Target interface.
func (s *SystemTarget) Equal(o spanconfig.Target) bool {
	if o.IsSpanTarget() {
		return false
	}

	return s.TargetTenantID == o.TargetProto().GetSystemSpanConfigTarget().TargetTenantID &&
		s.SourceTenantID == o.TargetProto().GetSystemSpanConfigTarget().SourceTenantID
}

// TargetProto implements the spanconfig.Target interface.
func (t *SystemTarget) TargetProto() *roachpb.SpanConfigTarget {
	return &roachpb.SpanConfigTarget{
		Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
			SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
				SourceTenantID: t.SourceTenantID,
				TargetTenantID: t.TargetTenantID,
			},
		},
	}
}

func (t *SystemTarget) validate() error {
	if t.SourceTenantID != roachpb.SystemTenantID && t.SourceTenantID != t.TargetTenantID {
		return errors.AssertionFailedf(
			"secondary tenant %s cannot target another tenant with ID %s",
			t.SourceTenantID,
			t.TargetTenantID,
		)
	}
	return nil
}

// DecodeSystemTarget converts the given span into a
// SystemTarget. An error is returned if the supplied span does not
// conform to the SystemSpanConfig encoding.
func DecodeSystemTarget(span roachpb.Span) (*SystemTarget, error) {
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigEntireKeyspace) {
		return NewSystemTarget(roachpb.SystemTenantID, roachpb.SystemTenantID)
	}

	// System span config was applied by the host tenant over a secondary
	// tenant's entire keyspace.
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace) {
		tenIDBytes := span.Key[len(keys.SystemSpanConfigHostOnTenantKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return nil, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return NewSystemTarget(roachpb.SystemTenantID, tenID)
	}

	// System span config was applied by a secondary tenant over its entire
	// keyspace.
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace) {
		tenIDBytes := span.Key[len(keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return nil, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return NewSystemTarget(tenID, tenID)
	}

	return nil,
		errors.AssertionFailedf("span %s did not conform to SystemTarget encoding", span)
}

// GetHostTenantInstalledSystemSpanConfigsEncoding returns keyspans which
// encompass all host tenant installed system span configurations; these may
// target both the entire keyspace or particular secondary tenants.
func GetHostTenantInstalledSystemSpanConfigsEncoding() roachpb.Spans {
	clusterTarget, err := NewSystemTarget(roachpb.SystemTenantID, roachpb.SystemTenantID)
	if err != nil {
		panic(err)
	}
	return roachpb.Spans{
		clusterTarget.Encode(),
		roachpb.Span{
			Key:    keys.SystemSpanConfigHostOnTenantKeyspace,
			EndKey: keys.SystemSpanConfigHostOnTenantKeyspace.PrefixEnd(),
		},
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
