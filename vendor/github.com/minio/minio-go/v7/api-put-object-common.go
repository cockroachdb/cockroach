/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 MinIO, Inc.
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
	"io"
	"math"
	"os"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

const nullVersionID = "null"

// Verify if reader is *minio.Object
func isObject(reader io.Reader) (ok bool) {
	_, ok = reader.(*Object)
	return
}

// Verify if reader is a generic ReaderAt
func isReadAt(reader io.Reader) (ok bool) {
	var v *os.File
	v, ok = reader.(*os.File)
	if ok {
		// Stdin, Stdout and Stderr all have *os.File type
		// which happen to also be io.ReaderAt compatible
		// we need to add special conditions for them to
		// be ignored by this function.
		for _, f := range []string{
			"/dev/stdin",
			"/dev/stdout",
			"/dev/stderr",
		} {
			if f == v.Name() {
				ok = false
				break
			}
		}
	} else {
		_, ok = reader.(io.ReaderAt)
	}
	return
}

// OptimalPartInfo - calculate the optimal part info for a given
// object size.
//
// NOTE: Assumption here is that for any object to be uploaded to any S3 compatible
// object storage it will have the following parameters as constants.
//
//  maxPartsCount - 10000
//  minPartSize - 16MiB
//  maxMultipartPutObjectSize - 5TiB
//
func OptimalPartInfo(objectSize int64, configuredPartSize uint64) (totalPartsCount int, partSize int64, lastPartSize int64, err error) {
	// object size is '-1' set it to 5TiB.
	var unknownSize bool
	if objectSize == -1 {
		unknownSize = true
		objectSize = maxMultipartPutObjectSize
	}

	// object size is larger than supported maximum.
	if objectSize > maxMultipartPutObjectSize {
		err = errEntityTooLarge(objectSize, maxMultipartPutObjectSize, "", "")
		return
	}

	var partSizeFlt float64
	if configuredPartSize > 0 {
		if int64(configuredPartSize) > objectSize {
			err = errEntityTooLarge(int64(configuredPartSize), objectSize, "", "")
			return
		}

		if !unknownSize {
			if objectSize > (int64(configuredPartSize) * maxPartsCount) {
				err = errInvalidArgument("Part size * max_parts(10000) is lesser than input objectSize.")
				return
			}
		}

		if configuredPartSize < absMinPartSize {
			err = errInvalidArgument("Input part size is smaller than allowed minimum of 5MiB.")
			return
		}

		if configuredPartSize > maxPartSize {
			err = errInvalidArgument("Input part size is bigger than allowed maximum of 5GiB.")
			return
		}

		partSizeFlt = float64(configuredPartSize)
		if unknownSize {
			// If input has unknown size and part size is configured
			// keep it to maximum allowed as per 10000 parts.
			objectSize = int64(configuredPartSize) * maxPartsCount
		}
	} else {
		configuredPartSize = minPartSize
		// Use floats for part size for all calculations to avoid
		// overflows during float64 to int64 conversions.
		partSizeFlt = float64(objectSize / maxPartsCount)
		partSizeFlt = math.Ceil(partSizeFlt/float64(configuredPartSize)) * float64(configuredPartSize)
	}

	// Total parts count.
	totalPartsCount = int(math.Ceil(float64(objectSize) / partSizeFlt))
	// Part size.
	partSize = int64(partSizeFlt)
	// Last part size.
	lastPartSize = objectSize - int64(totalPartsCount-1)*partSize
	return totalPartsCount, partSize, lastPartSize, nil
}

// getUploadID - fetch upload id if already present for an object name
// or initiate a new request to fetch a new upload id.
func (c *Client) newUploadID(ctx context.Context, bucketName, objectName string, opts PutObjectOptions) (uploadID string, err error) {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return "", err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return "", err
	}

	// Initiate multipart upload for an object.
	initMultipartUploadResult, err := c.initiateMultipartUpload(ctx, bucketName, objectName, opts)
	if err != nil {
		return "", err
	}
	return initMultipartUploadResult.UploadID, nil
}
