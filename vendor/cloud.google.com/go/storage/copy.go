// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/internal/trace"
	raw "google.golang.org/api/storage/v1"
)

// CopierFrom creates a Copier that can copy src to dst.
// You can immediately call Run on the returned Copier, or
// you can configure it first.
//
// For Requester Pays buckets, the user project of dst is billed, unless it is empty,
// in which case the user project of src is billed.
func (dst *ObjectHandle) CopierFrom(src *ObjectHandle) *Copier {
	return &Copier{dst: dst, src: src}
}

// A Copier copies a source object to a destination.
type Copier struct {
	// ObjectAttrs are optional attributes to set on the destination object.
	// Any attributes must be initialized before any calls on the Copier. Nil
	// or zero-valued attributes are ignored.
	ObjectAttrs

	// RewriteToken can be set before calling Run to resume a copy
	// operation. After Run returns a non-nil error, RewriteToken will
	// have been updated to contain the value needed to resume the copy.
	RewriteToken string

	// ProgressFunc can be used to monitor the progress of a multi-RPC copy
	// operation. If ProgressFunc is not nil and copying requires multiple
	// calls to the underlying service (see
	// https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite), then
	// ProgressFunc will be invoked after each call with the number of bytes of
	// content copied so far and the total size in bytes of the source object.
	//
	// ProgressFunc is intended to make upload progress available to the
	// application. For example, the implementation of ProgressFunc may update
	// a progress bar in the application's UI, or log the result of
	// float64(copiedBytes)/float64(totalBytes).
	//
	// ProgressFunc should return quickly without blocking.
	ProgressFunc func(copiedBytes, totalBytes uint64)

	// The Cloud KMS key, in the form projects/P/locations/L/keyRings/R/cryptoKeys/K,
	// that will be used to encrypt the object. Overrides the object's KMSKeyName, if
	// any.
	//
	// Providing both a DestinationKMSKeyName and a customer-supplied encryption key
	// (via ObjectHandle.Key) on the destination object will result in an error when
	// Run is called.
	DestinationKMSKeyName string

	dst, src *ObjectHandle
}

// Run performs the copy.
func (c *Copier) Run(ctx context.Context) (attrs *ObjectAttrs, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Copier.Run")
	defer func() { trace.EndSpan(ctx, err) }()

	if err := c.src.validate(); err != nil {
		return nil, err
	}
	if err := c.dst.validate(); err != nil {
		return nil, err
	}
	if c.DestinationKMSKeyName != "" && c.dst.encryptionKey != nil {
		return nil, errors.New("storage: cannot use DestinationKMSKeyName with a customer-supplied encryption key")
	}
	// Convert destination attributes to raw form, omitting the bucket.
	// If the bucket is included but name or content-type aren't, the service
	// returns a 400 with "Required" as the only message. Omitting the bucket
	// does not cause any problems.
	rawObject := c.ObjectAttrs.toRawObject("")
	for {
		res, err := c.callRewrite(ctx, rawObject)
		if err != nil {
			return nil, err
		}
		if c.ProgressFunc != nil {
			c.ProgressFunc(uint64(res.TotalBytesRewritten), uint64(res.ObjectSize))
		}
		if res.Done { // Finished successfully.
			return newObject(res.Resource), nil
		}
	}
}

func (c *Copier) callRewrite(ctx context.Context, rawObj *raw.Object) (*raw.RewriteResponse, error) {
	call := c.dst.c.raw.Objects.Rewrite(c.src.bucket, c.src.object, c.dst.bucket, c.dst.object, rawObj)

	call.Context(ctx).Projection("full")
	if c.RewriteToken != "" {
		call.RewriteToken(c.RewriteToken)
	}
	if c.DestinationKMSKeyName != "" {
		call.DestinationKmsKeyName(c.DestinationKMSKeyName)
	}
	if c.PredefinedACL != "" {
		call.DestinationPredefinedAcl(c.PredefinedACL)
	}
	if err := applyConds("Copy destination", c.dst.gen, c.dst.conds, call); err != nil {
		return nil, err
	}
	if c.dst.userProject != "" {
		call.UserProject(c.dst.userProject)
	} else if c.src.userProject != "" {
		call.UserProject(c.src.userProject)
	}
	if err := applySourceConds(c.src.gen, c.src.conds, call); err != nil {
		return nil, err
	}
	if err := setEncryptionHeaders(call.Header(), c.dst.encryptionKey, false); err != nil {
		return nil, err
	}
	if err := setEncryptionHeaders(call.Header(), c.src.encryptionKey, true); err != nil {
		return nil, err
	}
	var res *raw.RewriteResponse
	var err error
	setClientHeader(call.Header())

	retryCall := func() error { res, err = call.Do(); return err }
	isIdempotent := c.dst.conds != nil && (c.dst.conds.GenerationMatch != 0 || c.dst.conds.DoesNotExist)

	if err := run(ctx, retryCall, c.dst.retry, isIdempotent); err != nil {
		return nil, err
	}
	c.RewriteToken = res.RewriteToken
	return res, nil
}

// ComposerFrom creates a Composer that can compose srcs into dst.
// You can immediately call Run on the returned Composer, or you can
// configure it first.
//
// The encryption key for the destination object will be used to decrypt all
// source objects and encrypt the destination object. It is an error
// to specify an encryption key for any of the source objects.
func (dst *ObjectHandle) ComposerFrom(srcs ...*ObjectHandle) *Composer {
	return &Composer{dst: dst, srcs: srcs}
}

// A Composer composes source objects into a destination object.
//
// For Requester Pays buckets, the user project of dst is billed.
type Composer struct {
	// ObjectAttrs are optional attributes to set on the destination object.
	// Any attributes must be initialized before any calls on the Composer. Nil
	// or zero-valued attributes are ignored.
	ObjectAttrs

	// SendCRC specifies whether to transmit a CRC32C field. It should be set
	// to true in addition to setting the Composer's CRC32C field, because zero
	// is a valid CRC and normally a zero would not be transmitted.
	// If a CRC32C is sent, and the data in the destination object does not match
	// the checksum, the compose will be rejected.
	SendCRC32C bool

	dst  *ObjectHandle
	srcs []*ObjectHandle
}

// Run performs the compose operation.
func (c *Composer) Run(ctx context.Context) (attrs *ObjectAttrs, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Composer.Run")
	defer func() { trace.EndSpan(ctx, err) }()

	if err := c.dst.validate(); err != nil {
		return nil, err
	}
	if len(c.srcs) == 0 {
		return nil, errors.New("storage: at least one source object must be specified")
	}

	req := &raw.ComposeRequest{}
	// Compose requires a non-empty Destination, so we always set it,
	// even if the caller-provided ObjectAttrs is the zero value.
	req.Destination = c.ObjectAttrs.toRawObject(c.dst.bucket)
	if c.SendCRC32C {
		req.Destination.Crc32c = encodeUint32(c.ObjectAttrs.CRC32C)
	}
	for _, src := range c.srcs {
		if err := src.validate(); err != nil {
			return nil, err
		}
		if src.bucket != c.dst.bucket {
			return nil, fmt.Errorf("storage: all source objects must be in bucket %q, found %q", c.dst.bucket, src.bucket)
		}
		if src.encryptionKey != nil {
			return nil, fmt.Errorf("storage: compose source %s.%s must not have encryption key", src.bucket, src.object)
		}
		srcObj := &raw.ComposeRequestSourceObjects{
			Name: src.object,
		}
		if err := applyConds("ComposeFrom source", src.gen, src.conds, composeSourceObj{srcObj}); err != nil {
			return nil, err
		}
		req.SourceObjects = append(req.SourceObjects, srcObj)
	}

	call := c.dst.c.raw.Objects.Compose(c.dst.bucket, c.dst.object, req).Context(ctx)
	if err := applyConds("ComposeFrom destination", c.dst.gen, c.dst.conds, call); err != nil {
		return nil, err
	}
	if c.dst.userProject != "" {
		call.UserProject(c.dst.userProject)
	}
	if c.PredefinedACL != "" {
		call.DestinationPredefinedAcl(c.PredefinedACL)
	}
	if err := setEncryptionHeaders(call.Header(), c.dst.encryptionKey, false); err != nil {
		return nil, err
	}
	var obj *raw.Object
	setClientHeader(call.Header())

	retryCall := func() error { obj, err = call.Do(); return err }
	isIdempotent := c.dst.conds != nil && (c.dst.conds.GenerationMatch != 0 || c.dst.conds.DoesNotExist)

	if err := run(ctx, retryCall, c.dst.retry, isIdempotent); err != nil {
		return nil, err
	}
	return newObject(obj), nil
}
