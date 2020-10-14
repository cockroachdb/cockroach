// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"encoding/base64"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func encodeOIDCState(statePb serverpb.OIDCState) (string, error) {
	stateBytes, err := protoutil.Marshal(&statePb)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(stateBytes), nil
}

func decodeOIDCState(encodedState string) (*serverpb.OIDCState, error) {
	// Cookie value should be a base64 encoded protobuf.
	stateBytes, err := base64.URLEncoding.DecodeString(encodedState)
	if err != nil {
		return nil, errors.Wrap(err, "state could not be decoded")
	}
	var stateValue serverpb.OIDCState
	if err := protoutil.Unmarshal(stateBytes, &stateValue); err != nil {
		return nil, errors.Wrap(err, "state could not be unmarshaled")
	}
	return &stateValue, nil
}
