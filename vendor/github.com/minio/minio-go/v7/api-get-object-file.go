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
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// FGetObject - download contents of an object to a local file.
// The options can be used to specify the GET request further.
func (c *Client) FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts GetObjectOptions) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return err
	}

	// Verify if destination already exists.
	st, err := os.Stat(filePath)
	if err == nil {
		// If the destination exists and is a directory.
		if st.IsDir() {
			return errInvalidArgument("fileName is a directory.")
		}
	}

	// Proceed if file does not exist. return for all other errors.
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// Extract top level directory.
	objectDir, _ := filepath.Split(filePath)
	if objectDir != "" {
		// Create any missing top level directories.
		if err := os.MkdirAll(objectDir, 0700); err != nil {
			return err
		}
	}

	// Gather md5sum.
	objectStat, err := c.StatObject(ctx, bucketName, objectName, StatObjectOptions(opts))
	if err != nil {
		return err
	}

	// Write to a temporary file "fileName.part.minio" before saving.
	filePartPath := filePath + objectStat.ETag + ".part.minio"

	// If exists, open in append mode. If not create it as a part file.
	filePart, err := os.OpenFile(filePartPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	// If we return early with an error, be sure to close and delete
	// filePart.  If we have an error along the way there is a chance
	// that filePart is somehow damaged, and we should discard it.
	closeAndRemove := true
	defer func() {
		if closeAndRemove {
			_ = filePart.Close()
			_ = os.Remove(filePartPath)
		}
	}()

	// Issue Stat to get the current offset.
	st, err = filePart.Stat()
	if err != nil {
		return err
	}

	// Initialize get object request headers to set the
	// appropriate range offsets to read from.
	if st.Size() > 0 {
		opts.SetRange(st.Size(), 0)
	}

	// Seek to current position for incoming reader.
	objectReader, objectStat, _, err := c.getObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return err
	}

	// Write to the part file.
	if _, err = io.CopyN(filePart, objectReader, objectStat.Size); err != nil {
		return err
	}

	// Close the file before rename, this is specifically needed for Windows users.
	closeAndRemove = false
	if err = filePart.Close(); err != nil {
		return err
	}

	// Safely completed. Now commit by renaming to actual filename.
	if err = os.Rename(filePartPath, filePath); err != nil {
		return err
	}

	// Return.
	return nil
}
