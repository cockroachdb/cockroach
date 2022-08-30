/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2018 MinIO, Inc.
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

package minio

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
)

// Grantee represents the person being granted permissions.
type Grantee struct {
	XMLName     xml.Name `xml:"Grantee"`
	ID          string   `xml:"ID"`
	DisplayName string   `xml:"DisplayName"`
	URI         string   `xml:"URI"`
}

// Grant holds grant information
type Grant struct {
	XMLName    xml.Name `xml:"Grant"`
	Grantee    Grantee
	Permission string `xml:"Permission"`
}

// AccessControlList contains the set of grantees and the permissions assigned to each grantee.
type AccessControlList struct {
	XMLName    xml.Name `xml:"AccessControlList"`
	Grant      []Grant
	Permission string `xml:"Permission"`
}

type accessControlPolicy struct {
	XMLName           xml.Name `xml:"AccessControlPolicy"`
	Owner             Owner
	AccessControlList AccessControlList
}

// GetObjectACL get object ACLs
func (c *Client) GetObjectACL(ctx context.Context, bucketName, objectName string) (*ObjectInfo, error) {
	resp, err := c.executeMethod(ctx, http.MethodGet, requestMetadata{
		bucketName: bucketName,
		objectName: objectName,
		queryValues: url.Values{
			"acl": []string{""},
		},
	})
	if err != nil {
		return nil, err
	}
	defer closeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp, bucketName, objectName)
	}

	res := &accessControlPolicy{}

	if err := xmlDecoder(resp.Body, res); err != nil {
		return nil, err
	}

	objInfo, err := c.StatObject(ctx, bucketName, objectName, StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	objInfo.Owner.DisplayName = res.Owner.DisplayName
	objInfo.Owner.ID = res.Owner.ID

	objInfo.Grant = append(objInfo.Grant, res.AccessControlList.Grant...)

	cannedACL := getCannedACL(res)
	if cannedACL != "" {
		objInfo.Metadata.Add("X-Amz-Acl", cannedACL)
		return &objInfo, nil
	}

	grantACL := getAmzGrantACL(res)
	for k, v := range grantACL {
		objInfo.Metadata[k] = v
	}

	return &objInfo, nil
}

func getCannedACL(aCPolicy *accessControlPolicy) string {
	grants := aCPolicy.AccessControlList.Grant

	switch {
	case len(grants) == 1:
		if grants[0].Grantee.URI == "" && grants[0].Permission == "FULL_CONTROL" {
			return "private"
		}
	case len(grants) == 2:
		for _, g := range grants {
			if g.Grantee.URI == "http://acs.amazonaws.com/groups/global/AuthenticatedUsers" && g.Permission == "READ" {
				return "authenticated-read"
			}
			if g.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" && g.Permission == "READ" {
				return "public-read"
			}
			if g.Permission == "READ" && g.Grantee.ID == aCPolicy.Owner.ID {
				return "bucket-owner-read"
			}
		}
	case len(grants) == 3:
		for _, g := range grants {
			if g.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" && g.Permission == "WRITE" {
				return "public-read-write"
			}
		}
	}
	return ""
}

func getAmzGrantACL(aCPolicy *accessControlPolicy) map[string][]string {
	grants := aCPolicy.AccessControlList.Grant
	res := map[string][]string{}

	for _, g := range grants {
		switch {
		case g.Permission == "READ":
			res["X-Amz-Grant-Read"] = append(res["X-Amz-Grant-Read"], "id="+g.Grantee.ID)
		case g.Permission == "WRITE":
			res["X-Amz-Grant-Write"] = append(res["X-Amz-Grant-Write"], "id="+g.Grantee.ID)
		case g.Permission == "READ_ACP":
			res["X-Amz-Grant-Read-Acp"] = append(res["X-Amz-Grant-Read-Acp"], "id="+g.Grantee.ID)
		case g.Permission == "WRITE_ACP":
			res["X-Amz-Grant-Write-Acp"] = append(res["X-Amz-Grant-Write-Acp"], "id="+g.Grantee.ID)
		case g.Permission == "FULL_CONTROL":
			res["X-Amz-Grant-Full-Control"] = append(res["X-Amz-Grant-Full-Control"], "id="+g.Grantee.ID)
		}
	}
	return res
}
