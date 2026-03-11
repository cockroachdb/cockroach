// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import (
	"encoding/base64"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// RemoteDecodingInfoQueryParam is the query parameter key used to encode
// RemoteDecodingInfo into remote storage locator URIs.
const RemoteDecodingInfoQueryParam = "COCKROACH_INTERNAL_REMOTE_DECODING_INFO"

// InjectRemoteDecodingInfo injects a RemoteDecodingInfo proto into the given
// URI as a query parameter.
func InjectRemoteDecodingInfo(info RemoteDecodingInfo, uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return uri, err
	}

	bytes, err := protoutil.Marshal(&info)
	if err != nil {
		return "", err
	}
	encodedInfo := base64.URLEncoding.EncodeToString(bytes)
	vals := u.Query()
	vals.Set(RemoteDecodingInfoQueryParam, encodedInfo)
	u.RawQuery = vals.Encode()

	return u.String(), nil
}

// ExtractRemoteDecodingInfo extracts a RemoteDecodingInfo proto
// from the given URI, returning the cleaned URI and the decoding info.
func ExtractRemoteDecodingInfo(uri string) (string, RemoteDecodingInfo, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", RemoteDecodingInfo{}, err
	}

	infoStr := u.Query().Get(RemoteDecodingInfoQueryParam)
	if infoStr == "" {
		return uri, RemoteDecodingInfo{}, nil
	}

	infoBytes, err := base64.URLEncoding.DecodeString(infoStr)
	if err != nil {
		return "", RemoteDecodingInfo{}, err
	}

	var info RemoteDecodingInfo
	err = protoutil.Unmarshal(infoBytes, &info)
	if err != nil {
		return "", RemoteDecodingInfo{}, err
	}

	vals := u.Query()
	vals.Del(RemoteDecodingInfoQueryParam)
	u.RawQuery = vals.Encode()
	return u.String(), info, nil
}
