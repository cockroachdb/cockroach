// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clusterversionpb defines the ClusterVersion proto message and basic
// associated functionality.
package clusterversionpb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func (cv ClusterVersion) String() string {
	return redact.StringWithoutMarkers(cv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (cv ClusterVersion) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(cv.Version)
}

// Encode the cluster version (using the protobuf encoding).
func (cv ClusterVersion) Encode() []byte {
	encoded, err := protoutil.Marshal(&cv)
	if err != nil {
		// Marshal should never fail.
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "error marshalling version"))
	}
	return encoded
}

// Decode a cluster version that was encoded using Encode.
func Decode(encoded []byte) (ClusterVersion, error) {
	var cv ClusterVersion
	if err := protoutil.Unmarshal(encoded, &cv); err != nil {
		return ClusterVersion{}, err
	}
	return cv, nil
}

// ClusterVersionImpl implements the settings.ClusterVersionImpl interface.
// TODO(radu): remove this.
func (cv ClusterVersion) ClusterVersionImpl() {}

// EncodingFromVersionStr is a shorthand to generate an encoded cluster version
// from a version string.
func EncodingFromVersionStr(v string) ([]byte, error) {
	newV, err := roachpb.ParseVersion(v)
	if err != nil {
		return nil, err
	}
	newCV := ClusterVersion{Version: newV}
	return protoutil.Marshal(&newCV)
}
