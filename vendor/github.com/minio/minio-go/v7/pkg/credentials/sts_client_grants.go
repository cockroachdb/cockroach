/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package credentials

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

// AssumedRoleUser - The identifiers for the temporary security credentials that
// the operation returns. Please also see https://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumedRoleUser
type AssumedRoleUser struct {
	Arn           string
	AssumedRoleID string `xml:"AssumeRoleId"`
}

// AssumeRoleWithClientGrantsResponse contains the result of successful AssumeRoleWithClientGrants request.
type AssumeRoleWithClientGrantsResponse struct {
	XMLName          xml.Name           `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithClientGrantsResponse" json:"-"`
	Result           ClientGrantsResult `xml:"AssumeRoleWithClientGrantsResult"`
	ResponseMetadata struct {
		RequestID string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// ClientGrantsResult - Contains the response to a successful AssumeRoleWithClientGrants
// request, including temporary credentials that can be used to make MinIO API requests.
type ClientGrantsResult struct {
	AssumedRoleUser AssumedRoleUser `xml:",omitempty"`
	Audience        string          `xml:",omitempty"`
	Credentials     struct {
		AccessKey    string    `xml:"AccessKeyId" json:"accessKey,omitempty"`
		SecretKey    string    `xml:"SecretAccessKey" json:"secretKey,omitempty"`
		Expiration   time.Time `xml:"Expiration" json:"expiration,omitempty"`
		SessionToken string    `xml:"SessionToken" json:"sessionToken,omitempty"`
	} `xml:",omitempty"`
	PackedPolicySize             int    `xml:",omitempty"`
	Provider                     string `xml:",omitempty"`
	SubjectFromClientGrantsToken string `xml:",omitempty"`
}

// ClientGrantsToken - client grants token with expiry.
type ClientGrantsToken struct {
	Token  string
	Expiry int
}

// A STSClientGrants retrieves credentials from MinIO service, and keeps track if
// those credentials are expired.
type STSClientGrants struct {
	Expiry

	// Required http Client to use when connecting to MinIO STS service.
	Client *http.Client

	// MinIO endpoint to fetch STS credentials.
	STSEndpoint string

	// getClientGrantsTokenExpiry function to retrieve tokens
	// from IDP This function should return two values one is
	// accessToken which is a self contained access token (JWT)
	// and second return value is the expiry associated with
	// this token. This is a customer provided function and
	// is mandatory.
	GetClientGrantsTokenExpiry func() (*ClientGrantsToken, error)
}

// NewSTSClientGrants returns a pointer to a new
// Credentials object wrapping the STSClientGrants.
func NewSTSClientGrants(stsEndpoint string, getClientGrantsTokenExpiry func() (*ClientGrantsToken, error)) (*Credentials, error) {
	if stsEndpoint == "" {
		return nil, errors.New("STS endpoint cannot be empty")
	}
	if getClientGrantsTokenExpiry == nil {
		return nil, errors.New("Client grants access token and expiry retrieval function should be defined")
	}
	return New(&STSClientGrants{
		Client: &http.Client{
			Transport: http.DefaultTransport,
		},
		STSEndpoint:                stsEndpoint,
		GetClientGrantsTokenExpiry: getClientGrantsTokenExpiry,
	}), nil
}

func getClientGrantsCredentials(clnt *http.Client, endpoint string,
	getClientGrantsTokenExpiry func() (*ClientGrantsToken, error)) (AssumeRoleWithClientGrantsResponse, error) {

	accessToken, err := getClientGrantsTokenExpiry()
	if err != nil {
		return AssumeRoleWithClientGrantsResponse{}, err
	}

	v := url.Values{}
	v.Set("Action", "AssumeRoleWithClientGrants")
	v.Set("Token", accessToken.Token)
	v.Set("DurationSeconds", fmt.Sprintf("%d", accessToken.Expiry))
	v.Set("Version", STSVersion)

	u, err := url.Parse(endpoint)
	if err != nil {
		return AssumeRoleWithClientGrantsResponse{}, err
	}
	u.RawQuery = v.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return AssumeRoleWithClientGrantsResponse{}, err
	}
	resp, err := clnt.Do(req)
	if err != nil {
		return AssumeRoleWithClientGrantsResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return AssumeRoleWithClientGrantsResponse{}, err

		}
		_, err = xmlDecodeAndBody(bytes.NewReader(buf), &errResp)
		if err != nil {
			var s3Err Error
			if _, err = xmlDecodeAndBody(bytes.NewReader(buf), &s3Err); err != nil {
				return AssumeRoleWithClientGrantsResponse{}, err
			}
			errResp.RequestID = s3Err.RequestID
			errResp.STSError.Code = s3Err.Code
			errResp.STSError.Message = s3Err.Message
		}
		return AssumeRoleWithClientGrantsResponse{}, errResp
	}

	a := AssumeRoleWithClientGrantsResponse{}
	if err = xml.NewDecoder(resp.Body).Decode(&a); err != nil {
		return AssumeRoleWithClientGrantsResponse{}, err
	}
	return a, nil
}

// Retrieve retrieves credentials from the MinIO service.
// Error will be returned if the request fails.
func (m *STSClientGrants) Retrieve() (Value, error) {
	a, err := getClientGrantsCredentials(m.Client, m.STSEndpoint, m.GetClientGrantsTokenExpiry)
	if err != nil {
		return Value{}, err
	}

	// Expiry window is set to 10secs.
	m.SetExpiration(a.Result.Credentials.Expiration, DefaultExpiryWindow)

	return Value{
		AccessKeyID:     a.Result.Credentials.AccessKey,
		SecretAccessKey: a.Result.Credentials.SecretKey,
		SessionToken:    a.Result.Credentials.SessionToken,
		SignerType:      SignatureV4,
	}, nil
}
