// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package release

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Provider is an implementation of the ObjectPutGetter interface for S3
type S3Provider struct {
	service *s3.S3
	bucket  *string
}

// NewS3 creates a new instance of S3Provider
func NewS3(region string, bucket string) (*S3Provider, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return &S3Provider{}, err
	}
	return &S3Provider{
		service: s3.New(sess),
		bucket:  &bucket,
	}, nil
}

// GetObject implements object retrieval for S3
func (p *S3Provider) GetObject(input *GetObjectInput) (*GetObjectOutput, error) {
	obj, err := p.service.GetObject(&s3.GetObjectInput{
		Bucket: p.bucket,
		Key:    input.Key,
	})
	if err != nil {
		return &GetObjectOutput{}, err
	}
	return &GetObjectOutput{
		Body: obj.Body,
	}, nil
}

// PutObject implements object upload for S3
func (p *S3Provider) PutObject(input *PutObjectInput) error {
	putObjectInput := s3.PutObjectInput{
		Bucket:                  p.bucket,
		Key:                     input.Key,
		Body:                    input.Body,
		CacheControl:            input.CacheControl,
		ContentDisposition:      input.ContentDisposition,
		WebsiteRedirectLocation: input.WebsiteRedirectLocation,
	}
	if _, err := p.service.PutObject(&putObjectInput); err != nil {
		return fmt.Errorf("s3 upload %s: %w", *input.Key, err)
	}
	return nil
}

// Bucket returns bucket name
func (p *S3Provider) Bucket() string {
	return *p.bucket
}

// URL returns key's representation that can be used by AWS CLI
func (p S3Provider) URL(key string) string {
	return "s3://" + *p.bucket + "/" + strings.TrimPrefix(key, "/")
}
