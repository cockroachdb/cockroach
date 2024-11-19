// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package srvtestutils

import (
	"encoding/json"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// GetAdminJSONProto performs a RPC-over-HTTP request to the admin endpoint
// and unmarshals the response into the specified proto message.
func GetAdminJSONProto(
	ts serverutils.ApplicationLayerInterface, path string, response protoutil.Message,
) error {
	return GetAdminJSONProtoWithAdminOption(ts, path, response, true)
}

// GetAdminJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the admin endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func GetAdminJSONProtoWithAdminOption(
	ts serverutils.ApplicationLayerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, apiconstants.AdminPrefix+path, response, isAdmin)
}

// PostAdminJSONProto performs a RPC-over-HTTP request to the admin endpoint
// and unmarshals the response into the specified proto message.
func PostAdminJSONProto(
	ts serverutils.ApplicationLayerInterface, path string, request, response protoutil.Message,
) error {
	return PostAdminJSONProtoWithAdminOption(ts, path, request, response, true)
}

// PostAdminJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the admin endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func PostAdminJSONProtoWithAdminOption(
	ts serverutils.ApplicationLayerInterface,
	path string,
	request, response protoutil.Message,
	isAdmin bool,
) error {
	return serverutils.PostJSONProtoWithAdminOption(ts, apiconstants.AdminPrefix+path, request, response, isAdmin)
}

// GetStatusJSONProto performs a RPC-over-HTTP request to the status endpoint
// and unmarshals the response into the specified proto message.
func GetStatusJSONProto(
	ts serverutils.ApplicationLayerInterface, path string, response protoutil.Message,
) error {
	return serverutils.GetJSONProto(ts, apiconstants.StatusPrefix+path, response)
}

// PostStatusJSONProto performs a RPC-over-HTTP request to the status endpoint
// and unmarshals the response into the specified proto message.
func PostStatusJSONProto(
	ts serverutils.ApplicationLayerInterface, path string, request, response protoutil.Message,
) error {
	return serverutils.PostJSONProto(ts, apiconstants.StatusPrefix+path, request, response)
}

// GetStatusJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the status endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func GetStatusJSONProtoWithAdminOption(
	ts serverutils.ApplicationLayerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, apiconstants.StatusPrefix+path, response, isAdmin)
}

// GetStatusJSONProtoWithAdminAndTimeoutOption is similar to GetStatusJSONProtoWithAdminOption, but
// the caller can specify an additional timeout duration for the request.
func GetStatusJSONProtoWithAdminAndTimeoutOption(
	ts serverutils.ApplicationLayerInterface,
	path string,
	response protoutil.Message,
	isAdmin bool,
	additionalTimeout time.Duration,
) error {
	return serverutils.GetJSONProtoWithAdminAndTimeoutOption(ts, apiconstants.StatusPrefix+path, response, isAdmin, additionalTimeout)
}

// PostStatusJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the status endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func PostStatusJSONProtoWithAdminOption(
	ts serverutils.ApplicationLayerInterface,
	path string,
	request, response protoutil.Message,
	isAdmin bool,
) error {
	return serverutils.PostJSONProtoWithAdminOption(ts, apiconstants.StatusPrefix+path, request, response, isAdmin)
}

// GetText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func GetText(ts serverutils.ApplicationLayerInterface, url string) ([]byte, error) {
	httpClient, err := ts.GetAdminHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// GetJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func GetJSON(ts serverutils.ApplicationLayerInterface, url string) (interface{}, error) {
	body, err := GetText(ts, url)
	if err != nil {
		return nil, err
	}
	var jI interface{}
	if err := json.Unmarshal(body, &jI); err != nil {
		return nil, errors.Wrapf(err, "body is:\n%s", body)
	}
	return jI, nil
}
