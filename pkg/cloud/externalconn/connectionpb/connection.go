// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package connectionpb

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// RedactedDetails maps a ConnectionProvider to a method that returns the
// ConnectionDetails of that scheme with sensitive information redacted.
var RedactedDetails = map[ConnectionProvider]func(ConnectionDetails) (ConnectionDetails, error){}

// Type returns the ConnectionType of the receiver.
func (d *ConnectionDetails) Type() ConnectionType {
	switch d.Provider {
	case ConnectionProvider_TypeNodelocal:
		return TypeStorage
	case ConnectionProvider_TypeGSKMS:
		return TypeKMS
	case ConnectionProvider_TypeKafka:
		return TypeStorage
	default:
		panic(errors.AssertionFailedf("ConnectionDetails.Type called on a details with an unknown type: %T", d.Provider.String()))
	}
}

var _ jsonpb.JSONPBMarshaler = &ConnectionDetails{}

// MarshalJSONPB implements the jsonpb.Marshaler interface.
func (d *ConnectionDetails) MarshalJSONPB(marshaler *jsonpb.Marshaler) ([]byte, error) {
	if !protoreflect.ShouldRedact(marshaler) {
		return json.Marshal(d)
	}
	redact, ok := RedactedDetails[d.Provider]
	if !ok {
		return nil, errors.Newf("no redaction method registered for provider %s", d.Provider.String())
	}
	details, err := redact(*d)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to redact provider %s", d.Provider.String())
	}
	return json.Marshal(details)
}
