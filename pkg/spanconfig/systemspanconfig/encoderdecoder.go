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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/errwrap/testdata/src/github.com/cockroachdb/errors"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// EncodeTarget is used to convert a systemspanconfig.Target into a roachpb.Span
// suitable for persistence in the `system.span_configurations` table.
func EncodeTarget(target Target) roachpb.Span {
	var k roachpb.Key

	if target.TargeterTenantID == roachpb.SystemTenantID && target.TargeteeTenantID == roachpb.SystemTenantID {
		k = keys.SystemSpanConfigEntireKeyspace
	} else if target.TargeterTenantID == roachpb.SystemTenantID {
		k = encoding.EncodeUvarintAscending(
			keys.SystemSpanConfigHostOnTenantKeyspace, target.TargeteeTenantID.InternalValue,
		)
	} else {
		k = encoding.EncodeUvarintAscending(
			keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace, target.TargeterTenantID.InternalValue,
		)
	}
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

// DecodeTarget converts the given span into a systemspanconfig.Target. An error
// is returned if the span does not conform to the encoding used to store system
// span configs in `system.span_configurations`.
func DecodeTarget(span roachpb.Span) (Target, error) {
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigEntireKeyspace) {
		return MakeTarget(roachpb.SystemTenantID, roachpb.SystemTenantID)
	}

	// System span config was applied by the host tenant over a secondary
	// tenant's entire keyspace.
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigHostOnTenantKeyspace) {
		tenIDBytes := span.Key[len(keys.SystemSpanConfigHostOnTenantKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return Target{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeTarget(roachpb.SystemTenantID, tenID)
	}

	// System span config was applied by a secondary tenant over its entire
	// keyspace.
	if bytes.HasPrefix(span.Key, keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace) {
		tenIDBytes := span.Key[len(keys.SystemSpanConfigSecondaryTenantOnEntireKeyspace):]
		_, tenIDRaw, err := encoding.DecodeUvarintAscending(tenIDBytes)
		if err != nil {
			return Target{}, err
		}
		tenID := roachpb.MakeTenantID(tenIDRaw)
		return MakeTarget(tenID, tenID)
	}

	return Target{}, errors.AssertionFailedf("did not find prefix")
}
