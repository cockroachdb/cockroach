// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemspanconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// Target specifies the target of a system span configuration.
type Target struct {
	// SourceTenantID is the ID of the tenant that specified the system span
	// configuration.
	SourceTenantID roachpb.TenantID

	// TargetTenantID is the ID of the tenant that the associated system span
	// configuration applies to. If the host tenant is the source then the
	// system span configuration applies over all ranges in the system (including
	// those belonging to secondary tenants).
	//
	// Secondary tenants are only allowed to target themselves. The host tenant
	// may use this field to target a specific secondary tenant.
	TargetTenantID roachpb.TenantID
}

// MakeTargetUsingSourceContext constructs a new systemspanconfig.Target given a
// roachp.SystemSpanConfigTarget. The SourceTenantID is derived from the
// context passed in.
func MakeTargetUsingSourceContext(
	ctx context.Context, target roachpb.SystemSpanConfigTarget,
) (Target, error) {
	var sourceTenantID roachpb.TenantID
	var targetTenantID roachpb.TenantID
	if tenID, ok := roachpb.TenantFromContext(ctx); ok {
		sourceTenantID = tenID
	} else {
		sourceTenantID = roachpb.SystemTenantID
	}

	if target.TenantID != nil {
		targetTenantID = *target.TenantID
	} else {
		targetTenantID = sourceTenantID
	}
	return MakeTarget(sourceTenantID, targetTenantID)
}

// MakeTarget constructs a systemspanconfig.Target given a sourceTenantID and
// targetTenantID.
func MakeTarget(sourceTenantID roachpb.TenantID, targetTenantID roachpb.TenantID) (Target, error) {
	t := Target{
		SourceTenantID: sourceTenantID,
		TargetTenantID: targetTenantID,
	}
	return t, t.validate()
}

func (t *Target) validate() error {
	if t.SourceTenantID != roachpb.SystemTenantID && t.SourceTenantID != t.TargetTenantID {
		return errors.AssertionFailedf(
			"secondary tenant %s cannot target another tenant with ID %s",
			t.SourceTenantID,
			t.TargetTenantID,
		)
	}
	return nil
}
