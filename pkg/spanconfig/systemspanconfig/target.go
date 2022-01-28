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
	// TargeterTenantID is the ID of the tenant that specified the system span
	// configuration.
	TargeterTenantID roachpb.TenantID

	// TargeteeTenantID is the ID of the tenant that the associated system span
	// configuration applies to. If the host tenant is the targetee then the
	// system span configuration applies over all ranges in the system (including
	// those belonging to secondary tenants).
	//
	// Secondary tenants are only allowed to target themselves. The host tenant
	// may use this field to target a specific secondary tenant.
	TargeteeTenantID roachpb.TenantID
}

// MakeTargetUsingSourceContext constructs a new systemspanconfig.Target given a
// roachp.SystemSpanConfigTarget. The TargeterTenantID is derived from the
// context passed in.
func MakeTargetUsingSourceContext(
	ctx context.Context, target roachpb.SystemSpanConfigTarget,
) (Target, error) {
	var targeterTenantID roachpb.TenantID
	var targeteeTenantID roachpb.TenantID
	if tenID, ok := roachpb.TenantFromContext(ctx); ok {
		targeterTenantID = tenID
	} else {
		targeterTenantID = roachpb.SystemTenantID
	}

	if target.TenantID != nil {
		targeteeTenantID = *target.TenantID
	} else {
		targeteeTenantID = targeterTenantID
	}

	t := Target{
		TargeterTenantID: targeterTenantID,
		TargeteeTenantID: targeteeTenantID,
	}
	return t, t.validate()
}

// MakeTarget constructs a systemspanconfig.Target given a sourceTenantID and
// targetTenantID.
func MakeTarget(
	targeterTenantID roachpb.TenantID, targeteeTenantID roachpb.TenantID,
) (Target, error) {
	t := Target{
		TargeterTenantID: targeterTenantID,
		TargeteeTenantID: targeteeTenantID,
	}
	return t, t.validate()
}

func (t *Target) validate() error {
	if t.TargeterTenantID != roachpb.SystemTenantID && t.TargeterTenantID != t.TargeteeTenantID {
		return errors.AssertionFailedf(
			"secondary tenant %s cannot target another tenant with ID %s",
			t.TargeterTenantID,
			t.TargeteeTenantID,
		)
	}
	return nil
}
