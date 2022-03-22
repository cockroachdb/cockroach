// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoreflecttest

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/gogo/protobuf/jsonpb"
)

// SecretMessage is a message which should be redacted.
const SecretMessage = "secret message"

// RedactedMessage is the string the SecretMessage should be redacted to.
const RedactedMessage = "nothing to see here"

// MarshalJSONPB implements jsonpb.JSONPBMarshaler interface.
func (m Inner) MarshalJSONPB(marshaller *jsonpb.Marshaler) ([]byte, error) {
	if protoreflect.ShouldRedact(marshaller) && m.Value == SecretMessage {
		m.Value = RedactedMessage
	}
	return json.Marshal(m)
}
