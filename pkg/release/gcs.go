// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
)

// GCSProvider is an implementation of the ObjectPutGetter interface for GCS
type GCSProvider struct {
	client *storage.Client
	bucket string
}

// NewGCS creates a new instance of GCSProvider
func NewGCS(bucket string) (*GCSProvider, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return &GCSProvider{}, err
	}
	provider := &GCSProvider{
		client: client,
		bucket: bucket,
	}
	return provider, nil
}

// GetObject implements object retrieval for GCS
func (p *GCSProvider) GetObject(input *GetObjectInput) (*GetObjectOutput, error) {
	obj := p.client.Bucket(p.bucket).Object(*input.Key)
	ctx := context.Background()
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return &GetObjectOutput{}, err
	}
	return &GetObjectOutput{
		Body: reader,
	}, nil
}

// Bucket returns bucket name
func (p *GCSProvider) Bucket() string {
	return p.bucket
}

// PutObject implements object upload for GCS
func (p *GCSProvider) PutObject(input *PutObjectInput) error {
	obj := p.client.Bucket(p.bucket).Object(*input.Key)
	ctx := context.Background()
	var body []byte
	if input.WebsiteRedirectLocation != nil {
		// Seems like Google storage doesn't support this. Copy the original object.
		// copy content. Strip the leading slash to normalize the object name.
		copyFrom := strings.TrimPrefix(*input.WebsiteRedirectLocation, "/")
		r, err := p.client.Bucket(p.bucket).Object(copyFrom).NewReader(ctx)
		if err != nil {
			return fmt.Errorf("cannot read %s: %w", copyFrom, err)
		}
		body, err = io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("cannot download %s: %w", copyFrom, err)
		}
	} else {
		var err error
		body, err = io.ReadAll(input.Body)
		if err != nil {
			return fmt.Errorf("cannot read content: %w", err)
		}
	}
	wc := obj.NewWriter(ctx)
	if _, err := wc.Write(body); err != nil {
		return fmt.Errorf("error writing to GCS object %s: %w", *input.Key, err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("error closing GCS object %s: %w", *input.Key, err)
	}
	attrs := storage.ObjectAttrsToUpdate{}
	updateAttrs := false
	if input.ContentDisposition != nil {
		updateAttrs = true
		attrs.ContentDisposition = *input.ContentDisposition
	}
	if input.CacheControl != nil {
		updateAttrs = true
		attrs.CacheControl = *input.CacheControl
	}
	if updateAttrs {
		if _, err := obj.Update(ctx, attrs); err != nil {
			return fmt.Errorf("error updating attributes for %s: %w", *input.Key, err)
		}
	}
	return nil
}

// URL returns key's representation that can be used by gcsutil
func (p GCSProvider) URL(key string) string {
	return "gs://" + p.bucket + "/" + strings.TrimPrefix(key, "/")
}
