/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2020 MinIO, Inc.
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
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

/* **** SAMPLE ERROR RESPONSE ****
<?xml version="1.0" encoding="UTF-8"?>
<Error>
   <Code>AccessDenied</Code>
   <Message>Access Denied</Message>
   <BucketName>bucketName</BucketName>
   <Key>objectName</Key>
   <RequestId>F19772218238A85A</RequestId>
   <HostId>GuWkjyviSiGHizehqpmsD1ndz5NClSP19DOT+s2mv7gXGQ8/X1lhbDGiIJEXpGFD</HostId>
</Error>
*/

// ErrorResponse - Is the typed error returned by all API operations.
// ErrorResponse struct should be comparable since it is compared inside
// golang http API (https://github.com/golang/go/issues/29768)
type ErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string
	Message    string
	BucketName string
	Key        string
	Resource   string
	RequestID  string `xml:"RequestId"`
	HostID     string `xml:"HostId"`

	// Region where the bucket is located. This header is returned
	// only in HEAD bucket and ListObjects response.
	Region string

	// Captures the server string returned in response header.
	Server string

	// Underlying HTTP status code for the returned error
	StatusCode int `xml:"-" json:"-"`
}

// ToErrorResponse - Returns parsed ErrorResponse struct from body and
// http headers.
//
// For example:
//
//   import s3 "github.com/minio/minio-go/v7"
//   ...
//   ...
//   reader, stat, err := s3.GetObject(...)
//   if err != nil {
//      resp := s3.ToErrorResponse(err)
//   }
//   ...
func ToErrorResponse(err error) ErrorResponse {
	switch err := err.(type) {
	case ErrorResponse:
		return err
	default:
		return ErrorResponse{}
	}
}

// Error - Returns S3 error string.
func (e ErrorResponse) Error() string {
	if e.Message == "" {
		msg, ok := s3ErrorResponseMap[e.Code]
		if !ok {
			msg = fmt.Sprintf("Error response code %s.", e.Code)
		}
		return msg
	}
	return e.Message
}

// Common string for errors to report issue location in unexpected
// cases.
const (
	reportIssue = "Please report this issue at https://github.com/minio/minio-go/issues."
)

// xmlDecodeAndBody reads the whole body up to 1MB and
// tries to XML decode it into v.
// The body that was read and any error from reading or decoding is returned.
func xmlDecodeAndBody(bodyReader io.Reader, v interface{}) ([]byte, error) {
	// read the whole body (up to 1MB)
	const maxBodyLength = 1 << 20
	body, err := ioutil.ReadAll(io.LimitReader(bodyReader, maxBodyLength))
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(body), xmlDecoder(bytes.NewReader(body), v)
}

// httpRespToErrorResponse returns a new encoded ErrorResponse
// structure as error.
func httpRespToErrorResponse(resp *http.Response, bucketName, objectName string) error {
	if resp == nil {
		msg := "Empty http response. " + reportIssue
		return errInvalidArgument(msg)
	}

	errResp := ErrorResponse{
		StatusCode: resp.StatusCode,
		Server:     resp.Header.Get("Server"),
	}

	errBody, err := xmlDecodeAndBody(resp.Body, &errResp)
	// Xml decoding failed with no body, fall back to HTTP headers.
	if err != nil {
		switch resp.StatusCode {
		case http.StatusNotFound:
			if objectName == "" {
				errResp = ErrorResponse{
					StatusCode: resp.StatusCode,
					Code:       "NoSuchBucket",
					Message:    "The specified bucket does not exist.",
					BucketName: bucketName,
				}
			} else {
				errResp = ErrorResponse{
					StatusCode: resp.StatusCode,
					Code:       "NoSuchKey",
					Message:    "The specified key does not exist.",
					BucketName: bucketName,
					Key:        objectName,
				}
			}
		case http.StatusForbidden:
			errResp = ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       "AccessDenied",
				Message:    "Access Denied.",
				BucketName: bucketName,
				Key:        objectName,
			}
		case http.StatusConflict:
			errResp = ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       "Conflict",
				Message:    "Bucket not empty.",
				BucketName: bucketName,
			}
		case http.StatusPreconditionFailed:
			errResp = ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       "PreconditionFailed",
				Message:    s3ErrorResponseMap["PreconditionFailed"],
				BucketName: bucketName,
				Key:        objectName,
			}
		default:
			msg := resp.Status
			if len(errBody) > 0 {
				msg = string(errBody)
				if len(msg) > 1024 {
					msg = msg[:1024] + "..."
				}
			}
			errResp = ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       resp.Status,
				Message:    msg,
				BucketName: bucketName,
			}
		}
	}

	// Save hostID, requestID and region information
	// from headers if not available through error XML.
	if errResp.RequestID == "" {
		errResp.RequestID = resp.Header.Get("x-amz-request-id")
	}
	if errResp.HostID == "" {
		errResp.HostID = resp.Header.Get("x-amz-id-2")
	}
	if errResp.Region == "" {
		errResp.Region = resp.Header.Get("x-amz-bucket-region")
	}
	if errResp.Code == "InvalidRegion" && errResp.Region != "" {
		errResp.Message = fmt.Sprintf("Region does not match, expecting region ‘%s’.", errResp.Region)
	}

	return errResp
}

// errTransferAccelerationBucket - bucket name is invalid to be used with transfer acceleration.
func errTransferAccelerationBucket(bucketName string) error {
	return ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "InvalidArgument",
		Message:    "The name of the bucket used for Transfer Acceleration must be DNS-compliant and must not contain periods ‘.’.",
		BucketName: bucketName,
	}
}

// errEntityTooLarge - Input size is larger than supported maximum.
func errEntityTooLarge(totalSize, maxObjectSize int64, bucketName, objectName string) error {
	msg := fmt.Sprintf("Your proposed upload size ‘%d’ exceeds the maximum allowed object size ‘%d’ for single PUT operation.", totalSize, maxObjectSize)
	return ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "EntityTooLarge",
		Message:    msg,
		BucketName: bucketName,
		Key:        objectName,
	}
}

// errEntityTooSmall - Input size is smaller than supported minimum.
func errEntityTooSmall(totalSize int64, bucketName, objectName string) error {
	msg := fmt.Sprintf("Your proposed upload size ‘%d’ is below the minimum allowed object size ‘0B’ for single PUT operation.", totalSize)
	return ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "EntityTooSmall",
		Message:    msg,
		BucketName: bucketName,
		Key:        objectName,
	}
}

// errUnexpectedEOF - Unexpected end of file reached.
func errUnexpectedEOF(totalRead, totalSize int64, bucketName, objectName string) error {
	msg := fmt.Sprintf("Data read ‘%d’ is not equal to the size ‘%d’ of the input Reader.", totalRead, totalSize)
	return ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "UnexpectedEOF",
		Message:    msg,
		BucketName: bucketName,
		Key:        objectName,
	}
}

// errInvalidBucketName - Invalid bucket name response.
func errInvalidBucketName(message string) error {
	return ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "InvalidBucketName",
		Message:    message,
		RequestID:  "minio",
	}
}

// errInvalidObjectName - Invalid object name response.
func errInvalidObjectName(message string) error {
	return ErrorResponse{
		StatusCode: http.StatusNotFound,
		Code:       "NoSuchKey",
		Message:    message,
		RequestID:  "minio",
	}
}

// errInvalidArgument - Invalid argument response.
func errInvalidArgument(message string) error {
	return ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "InvalidArgument",
		Message:    message,
		RequestID:  "minio",
	}
}

// errAPINotSupported - API not supported response
// The specified API call is not supported
func errAPINotSupported(message string) error {
	return ErrorResponse{
		StatusCode: http.StatusNotImplemented,
		Code:       "APINotSupported",
		Message:    message,
		RequestID:  "minio",
	}
}
