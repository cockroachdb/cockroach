// Copyright 2014 Google LLC
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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/internal/optional"
	"cloud.google.com/go/internal/trace"
	"golang.org/x/xerrors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	raw "google.golang.org/api/storage/v1"
)

// BucketHandle provides operations on a Google Cloud Storage bucket.
// Use Client.Bucket to get a handle.
type BucketHandle struct {
	c                *Client
	name             string
	acl              ACLHandle
	defaultObjectACL ACLHandle
	conds            *BucketConditions
	userProject      string // project for Requester Pays buckets
	retry            *retryConfig
}

// Bucket returns a BucketHandle, which provides operations on the named bucket.
// This call does not perform any network operations.
//
// The supplied name must contain only lowercase letters, numbers, dashes,
// underscores, and dots. The full specification for valid bucket names can be
// found at:
//   https://cloud.google.com/storage/docs/bucket-naming
func (c *Client) Bucket(name string) *BucketHandle {
	retry := c.retry.clone()
	return &BucketHandle{
		c:    c,
		name: name,
		acl: ACLHandle{
			c:      c,
			bucket: name,
			retry:  retry,
		},
		defaultObjectACL: ACLHandle{
			c:         c,
			bucket:    name,
			isDefault: true,
			retry:     retry,
		},
		retry: retry,
	}
}

// Create creates the Bucket in the project.
// If attrs is nil the API defaults will be used.
func (b *BucketHandle) Create(ctx context.Context, projectID string, attrs *BucketAttrs) (err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Bucket.Create")
	defer func() { trace.EndSpan(ctx, err) }()

	var bkt *raw.Bucket
	if attrs != nil {
		bkt = attrs.toRawBucket()
	} else {
		bkt = &raw.Bucket{}
	}
	bkt.Name = b.name
	// If there is lifecycle information but no location, explicitly set
	// the location. This is a GCS quirk/bug.
	if bkt.Location == "" && bkt.Lifecycle != nil {
		bkt.Location = "US"
	}
	req := b.c.raw.Buckets.Insert(projectID, bkt)
	setClientHeader(req.Header())
	if attrs != nil && attrs.PredefinedACL != "" {
		req.PredefinedAcl(attrs.PredefinedACL)
	}
	if attrs != nil && attrs.PredefinedDefaultObjectACL != "" {
		req.PredefinedDefaultObjectAcl(attrs.PredefinedDefaultObjectACL)
	}
	return run(ctx, func() error { _, err := req.Context(ctx).Do(); return err }, b.retry, true)
}

// Delete deletes the Bucket.
func (b *BucketHandle) Delete(ctx context.Context) (err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Bucket.Delete")
	defer func() { trace.EndSpan(ctx, err) }()

	req, err := b.newDeleteCall()
	if err != nil {
		return err
	}

	return run(ctx, func() error { return req.Context(ctx).Do() }, b.retry, true)
}

func (b *BucketHandle) newDeleteCall() (*raw.BucketsDeleteCall, error) {
	req := b.c.raw.Buckets.Delete(b.name)
	setClientHeader(req.Header())
	if err := applyBucketConds("BucketHandle.Delete", b.conds, req); err != nil {
		return nil, err
	}
	if b.userProject != "" {
		req.UserProject(b.userProject)
	}
	return req, nil
}

// ACL returns an ACLHandle, which provides access to the bucket's access control list.
// This controls who can list, create or overwrite the objects in a bucket.
// This call does not perform any network operations.
func (b *BucketHandle) ACL() *ACLHandle {
	return &b.acl
}

// DefaultObjectACL returns an ACLHandle, which provides access to the bucket's default object ACLs.
// These ACLs are applied to newly created objects in this bucket that do not have a defined ACL.
// This call does not perform any network operations.
func (b *BucketHandle) DefaultObjectACL() *ACLHandle {
	return &b.defaultObjectACL
}

// Object returns an ObjectHandle, which provides operations on the named object.
// This call does not perform any network operations such as fetching the object or verifying its existence.
// Use methods on ObjectHandle to perform network operations.
//
// name must consist entirely of valid UTF-8-encoded runes. The full specification
// for valid object names can be found at:
//   https://cloud.google.com/storage/docs/naming-objects
func (b *BucketHandle) Object(name string) *ObjectHandle {
	retry := b.retry.clone()
	return &ObjectHandle{
		c:      b.c,
		bucket: b.name,
		object: name,
		acl: ACLHandle{
			c:           b.c,
			bucket:      b.name,
			object:      name,
			userProject: b.userProject,
			retry:       retry,
		},
		gen:         -1,
		userProject: b.userProject,
		retry:       retry,
	}
}

// Attrs returns the metadata for the bucket.
func (b *BucketHandle) Attrs(ctx context.Context) (attrs *BucketAttrs, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Bucket.Attrs")
	defer func() { trace.EndSpan(ctx, err) }()

	req, err := b.newGetCall()
	if err != nil {
		return nil, err
	}
	var resp *raw.Bucket
	err = run(ctx, func() error {
		resp, err = req.Context(ctx).Do()
		return err
	}, b.retry, true)
	var e *googleapi.Error
	if ok := xerrors.As(err, &e); ok && e.Code == http.StatusNotFound {
		return nil, ErrBucketNotExist
	}
	if err != nil {
		return nil, err
	}
	return newBucket(resp)
}

func (b *BucketHandle) newGetCall() (*raw.BucketsGetCall, error) {
	req := b.c.raw.Buckets.Get(b.name).Projection("full")
	setClientHeader(req.Header())
	if err := applyBucketConds("BucketHandle.Attrs", b.conds, req); err != nil {
		return nil, err
	}
	if b.userProject != "" {
		req.UserProject(b.userProject)
	}
	return req, nil
}

// Update updates a bucket's attributes.
func (b *BucketHandle) Update(ctx context.Context, uattrs BucketAttrsToUpdate) (attrs *BucketAttrs, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Bucket.Create")
	defer func() { trace.EndSpan(ctx, err) }()

	req, err := b.newPatchCall(&uattrs)
	if err != nil {
		return nil, err
	}
	if uattrs.PredefinedACL != "" {
		req.PredefinedAcl(uattrs.PredefinedACL)
	}
	if uattrs.PredefinedDefaultObjectACL != "" {
		req.PredefinedDefaultObjectAcl(uattrs.PredefinedDefaultObjectACL)
	}

	isIdempotent := b.conds != nil && b.conds.MetagenerationMatch != 0

	var rawBucket *raw.Bucket
	call := func() error {
		rb, err := req.Context(ctx).Do()
		rawBucket = rb
		return err
	}

	if err := run(ctx, call, b.retry, isIdempotent); err != nil {
		return nil, err
	}
	return newBucket(rawBucket)
}

func (b *BucketHandle) newPatchCall(uattrs *BucketAttrsToUpdate) (*raw.BucketsPatchCall, error) {
	rb := uattrs.toRawBucket()
	req := b.c.raw.Buckets.Patch(b.name, rb).Projection("full")
	setClientHeader(req.Header())
	if err := applyBucketConds("BucketHandle.Update", b.conds, req); err != nil {
		return nil, err
	}
	if b.userProject != "" {
		req.UserProject(b.userProject)
	}
	return req, nil
}

// SignedURL returns a URL for the specified object. Signed URLs allow anyone
// access to a restricted resource for a limited time without needing a
// Google account or signing in. For more information about signed URLs, see
// https://cloud.google.com/storage/docs/accesscontrol#signed_urls_query_string_authentication
//
// This method only requires the Method and Expires fields in the specified
// SignedURLOptions opts to be non-nil. If not provided, it attempts to fill the
// GoogleAccessID and PrivateKey from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
// If you are authenticating with a custom HTTP client, Service Account based
// auto-detection will be hindered.
//
// If no private key is found, it attempts to use the GoogleAccessID to sign the URL.
// This requires the IAM Service Account Credentials API to be enabled
// (https://console.developers.google.com/apis/api/iamcredentials.googleapis.com/overview)
// and iam.serviceAccounts.signBlob permissions on the GoogleAccessID service account.
// If you do not want these fields set for you, you may pass them in through opts or use
// SignedURL(bucket, name string, opts *SignedURLOptions) instead.
func (b *BucketHandle) SignedURL(object string, opts *SignedURLOptions) (string, error) {
	if opts.GoogleAccessID != "" && (opts.SignBytes != nil || len(opts.PrivateKey) > 0) {
		return SignedURL(b.name, object, opts)
	}
	// Make a copy of opts so we don't modify the pointer parameter.
	newopts := opts.clone()

	if newopts.GoogleAccessID == "" {
		id, err := b.detectDefaultGoogleAccessID()
		if err != nil {
			return "", err
		}
		newopts.GoogleAccessID = id
	}
	if newopts.SignBytes == nil && len(newopts.PrivateKey) == 0 {
		if b.c.creds != nil && len(b.c.creds.JSON) > 0 {
			var sa struct {
				PrivateKey string `json:"private_key"`
			}
			err := json.Unmarshal(b.c.creds.JSON, &sa)
			if err == nil && sa.PrivateKey != "" {
				newopts.PrivateKey = []byte(sa.PrivateKey)
			}
		}

		// Don't error out if we can't unmarshal the private key from the client,
		// fallback to the default sign function for the service account.
		if len(newopts.PrivateKey) == 0 {
			newopts.SignBytes = b.defaultSignBytesFunc(newopts.GoogleAccessID)
		}
	}
	return SignedURL(b.name, object, newopts)
}

// GenerateSignedPostPolicyV4 generates a PostPolicyV4 value from bucket, object and opts.
// The generated URL and fields will then allow an unauthenticated client to perform multipart uploads.
//
// This method only requires the Expires field in the specified PostPolicyV4Options
// to be non-nil. If not provided, it attempts to fill the GoogleAccessID and PrivateKey
// from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
// If you are authenticating with a custom HTTP client, Service Account based
// auto-detection will be hindered.
//
// If no private key is found, it attempts to use the GoogleAccessID to sign the URL.
// This requires the IAM Service Account Credentials API to be enabled
// (https://console.developers.google.com/apis/api/iamcredentials.googleapis.com/overview)
// and iam.serviceAccounts.signBlob permissions on the GoogleAccessID service account.
// If you do not want these fields set for you, you may pass them in through opts or use
// GenerateSignedPostPolicyV4(bucket, name string, opts *PostPolicyV4Options) instead.
func (b *BucketHandle) GenerateSignedPostPolicyV4(object string, opts *PostPolicyV4Options) (*PostPolicyV4, error) {
	if opts.GoogleAccessID != "" && (opts.SignRawBytes != nil || opts.SignBytes != nil || len(opts.PrivateKey) > 0) {
		return GenerateSignedPostPolicyV4(b.name, object, opts)
	}
	// Make a copy of opts so we don't modify the pointer parameter.
	newopts := opts.clone()

	if newopts.GoogleAccessID == "" {
		id, err := b.detectDefaultGoogleAccessID()
		if err != nil {
			return nil, err
		}
		newopts.GoogleAccessID = id
	}
	if newopts.SignBytes == nil && newopts.SignRawBytes == nil && len(newopts.PrivateKey) == 0 {
		if b.c.creds != nil && len(b.c.creds.JSON) > 0 {
			var sa struct {
				PrivateKey string `json:"private_key"`
			}
			err := json.Unmarshal(b.c.creds.JSON, &sa)
			if err == nil && sa.PrivateKey != "" {
				newopts.PrivateKey = []byte(sa.PrivateKey)
			}
		}

		// Don't error out if we can't unmarshal the private key from the client,
		// fallback to the default sign function for the service account.
		if len(newopts.PrivateKey) == 0 {
			newopts.SignRawBytes = b.defaultSignBytesFunc(newopts.GoogleAccessID)
		}
	}
	return GenerateSignedPostPolicyV4(b.name, object, newopts)
}

func (b *BucketHandle) detectDefaultGoogleAccessID() (string, error) {
	returnErr := errors.New("no credentials found on client and not on GCE (Google Compute Engine)")

	if b.c.creds != nil && len(b.c.creds.JSON) > 0 {
		var sa struct {
			ClientEmail string `json:"client_email"`
		}
		err := json.Unmarshal(b.c.creds.JSON, &sa)
		if err == nil && sa.ClientEmail != "" {
			return sa.ClientEmail, nil
		} else if err != nil {
			returnErr = err
		} else {
			returnErr = errors.New("storage: empty client email in credentials")
		}

	}

	// Don't error out if we can't unmarshal, fallback to GCE check.
	if metadata.OnGCE() {
		email, err := metadata.Email("default")
		if err == nil && email != "" {
			return email, nil
		} else if err != nil {
			returnErr = err
		} else {
			returnErr = errors.New("got empty email from GCE metadata service")
		}

	}
	return "", fmt.Errorf("storage: unable to detect default GoogleAccessID: %v", returnErr)
}

func (b *BucketHandle) defaultSignBytesFunc(email string) func([]byte) ([]byte, error) {
	return func(in []byte) ([]byte, error) {
		ctx := context.Background()

		// It's ok to recreate this service per call since we pass in the http client,
		// circumventing the cost of recreating the auth/transport layer
		svc, err := iamcredentials.NewService(ctx, option.WithHTTPClient(b.c.hc))
		if err != nil {
			return nil, fmt.Errorf("unable to create iamcredentials client: %v", err)
		}

		resp, err := svc.Projects.ServiceAccounts.SignBlob(fmt.Sprintf("projects/-/serviceAccounts/%s", email), &iamcredentials.SignBlobRequest{
			Payload: base64.StdEncoding.EncodeToString(in),
		}).Do()
		if err != nil {
			return nil, fmt.Errorf("unable to sign bytes: %v", err)
		}
		out, err := base64.StdEncoding.DecodeString(resp.SignedBlob)
		if err != nil {
			return nil, fmt.Errorf("unable to base64 decode response: %v", err)
		}
		return out, nil
	}
}

// BucketAttrs represents the metadata for a Google Cloud Storage bucket.
// Read-only fields are ignored by BucketHandle.Create.
type BucketAttrs struct {
	// Name is the name of the bucket.
	// This field is read-only.
	Name string

	// ACL is the list of access control rules on the bucket.
	ACL []ACLRule

	// BucketPolicyOnly is an alias for UniformBucketLevelAccess. Use of
	// UniformBucketLevelAccess is recommended above the use of this field.
	// Setting BucketPolicyOnly.Enabled OR UniformBucketLevelAccess.Enabled to
	// true, will enable UniformBucketLevelAccess.
	BucketPolicyOnly BucketPolicyOnly

	// UniformBucketLevelAccess configures access checks to use only bucket-level IAM
	// policies and ignore any ACL rules for the bucket.
	// See https://cloud.google.com/storage/docs/uniform-bucket-level-access
	// for more information.
	UniformBucketLevelAccess UniformBucketLevelAccess

	// PublicAccessPrevention is the setting for the bucket's
	// PublicAccessPrevention policy, which can be used to prevent public access
	// of data in the bucket. See
	// https://cloud.google.com/storage/docs/public-access-prevention for more
	// information.
	PublicAccessPrevention PublicAccessPrevention

	// DefaultObjectACL is the list of access controls to
	// apply to new objects when no object ACL is provided.
	DefaultObjectACL []ACLRule

	// DefaultEventBasedHold is the default value for event-based hold on
	// newly created objects in this bucket. It defaults to false.
	DefaultEventBasedHold bool

	// If not empty, applies a predefined set of access controls. It should be set
	// only when creating a bucket.
	// It is always empty for BucketAttrs returned from the service.
	// See https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
	// for valid values.
	PredefinedACL string

	// If not empty, applies a predefined set of default object access controls.
	// It should be set only when creating a bucket.
	// It is always empty for BucketAttrs returned from the service.
	// See https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
	// for valid values.
	PredefinedDefaultObjectACL string

	// Location is the location of the bucket. It defaults to "US".
	Location string

	// MetaGeneration is the metadata generation of the bucket.
	// This field is read-only.
	MetaGeneration int64

	// StorageClass is the default storage class of the bucket. This defines
	// how objects in the bucket are stored and determines the SLA
	// and the cost of storage. Typical values are "STANDARD", "NEARLINE",
	// "COLDLINE" and "ARCHIVE". Defaults to "STANDARD".
	// See https://cloud.google.com/storage/docs/storage-classes for all
	// valid values.
	StorageClass string

	// Created is the creation time of the bucket.
	// This field is read-only.
	Created time.Time

	// VersioningEnabled reports whether this bucket has versioning enabled.
	VersioningEnabled bool

	// Labels are the bucket's labels.
	Labels map[string]string

	// RequesterPays reports whether the bucket is a Requester Pays bucket.
	// Clients performing operations on Requester Pays buckets must provide
	// a user project (see BucketHandle.UserProject), which will be billed
	// for the operations.
	RequesterPays bool

	// Lifecycle is the lifecycle configuration for objects in the bucket.
	Lifecycle Lifecycle

	// Retention policy enforces a minimum retention time for all objects
	// contained in the bucket. A RetentionPolicy of nil implies the bucket
	// has no minimum data retention.
	//
	// This feature is in private alpha release. It is not currently available to
	// most customers. It might be changed in backwards-incompatible ways and is not
	// subject to any SLA or deprecation policy.
	RetentionPolicy *RetentionPolicy

	// The bucket's Cross-Origin Resource Sharing (CORS) configuration.
	CORS []CORS

	// The encryption configuration used by default for newly inserted objects.
	Encryption *BucketEncryption

	// The logging configuration.
	Logging *BucketLogging

	// The website configuration.
	Website *BucketWebsite

	// Etag is the HTTP/1.1 Entity tag for the bucket.
	// This field is read-only.
	Etag string

	// LocationType describes how data is stored and replicated.
	// Typical values are "multi-region", "region" and "dual-region".
	// This field is read-only.
	LocationType string

	// The project number of the project the bucket belongs to.
	// This field is read-only.
	ProjectNumber uint64

	// RPO configures the Recovery Point Objective (RPO) policy of the bucket.
	// Set to RPOAsyncTurbo to turn on Turbo Replication for a bucket.
	// See https://cloud.google.com/storage/docs/managing-turbo-replication for
	// more information.
	RPO RPO
}

// BucketPolicyOnly is an alias for UniformBucketLevelAccess.
// Use of UniformBucketLevelAccess is preferred above BucketPolicyOnly.
type BucketPolicyOnly struct {
	// Enabled specifies whether access checks use only bucket-level IAM
	// policies. Enabled may be disabled until the locked time.
	Enabled bool
	// LockedTime specifies the deadline for changing Enabled from true to
	// false.
	LockedTime time.Time
}

// UniformBucketLevelAccess configures access checks to use only bucket-level IAM
// policies.
type UniformBucketLevelAccess struct {
	// Enabled specifies whether access checks use only bucket-level IAM
	// policies. Enabled may be disabled until the locked time.
	Enabled bool
	// LockedTime specifies the deadline for changing Enabled from true to
	// false.
	LockedTime time.Time
}

// PublicAccessPrevention configures the Public Access Prevention feature, which
// can be used to disallow public access to any data in a bucket. See
// https://cloud.google.com/storage/docs/public-access-prevention for more
// information.
type PublicAccessPrevention int

const (
	// PublicAccessPreventionUnknown is a zero value, used only if this field is
	// not set in a call to GCS.
	PublicAccessPreventionUnknown PublicAccessPrevention = iota

	// PublicAccessPreventionUnspecified corresponds to a value of "unspecified".
	// Deprecated: use PublicAccessPreventionInherited
	PublicAccessPreventionUnspecified

	// PublicAccessPreventionEnforced corresponds to a value of "enforced". This
	// enforces Public Access Prevention on the bucket.
	PublicAccessPreventionEnforced

	// PublicAccessPreventionInherited corresponds to a value of "inherited"
	// and is the default for buckets.
	PublicAccessPreventionInherited

	publicAccessPreventionUnknown string = ""
	// TODO: remove unspecified when change is fully completed
	publicAccessPreventionUnspecified = "unspecified"
	publicAccessPreventionEnforced    = "enforced"
	publicAccessPreventionInherited   = "inherited"
)

func (p PublicAccessPrevention) String() string {
	switch p {
	case PublicAccessPreventionInherited, PublicAccessPreventionUnspecified:
		return publicAccessPreventionInherited
	case PublicAccessPreventionEnforced:
		return publicAccessPreventionEnforced
	default:
		return publicAccessPreventionUnknown
	}
}

// Lifecycle is the lifecycle configuration for objects in the bucket.
type Lifecycle struct {
	Rules []LifecycleRule
}

// RetentionPolicy enforces a minimum retention time for all objects
// contained in the bucket.
//
// Any attempt to overwrite or delete objects younger than the retention
// period will result in an error. An unlocked retention policy can be
// modified or removed from the bucket via the Update method. A
// locked retention policy cannot be removed or shortened in duration
// for the lifetime of the bucket.
//
// This feature is in private alpha release. It is not currently available to
// most customers. It might be changed in backwards-incompatible ways and is not
// subject to any SLA or deprecation policy.
type RetentionPolicy struct {
	// RetentionPeriod specifies the duration that objects need to be
	// retained. Retention duration must be greater than zero and less than
	// 100 years. Note that enforcement of retention periods less than a day
	// is not guaranteed. Such periods should only be used for testing
	// purposes.
	RetentionPeriod time.Duration

	// EffectiveTime is the time from which the policy was enforced and
	// effective. This field is read-only.
	EffectiveTime time.Time

	// IsLocked describes whether the bucket is locked. Once locked, an
	// object retention policy cannot be modified.
	// This field is read-only.
	IsLocked bool
}

const (
	// RFC3339 timestamp with only the date segment, used for CreatedBefore,
	// CustomTimeBefore, and NoncurrentTimeBefore in LifecycleRule.
	rfc3339Date = "2006-01-02"

	// DeleteAction is a lifecycle action that deletes a live and/or archived
	// objects. Takes precedence over SetStorageClass actions.
	DeleteAction = "Delete"

	// SetStorageClassAction changes the storage class of live and/or archived
	// objects.
	SetStorageClassAction = "SetStorageClass"
)

// LifecycleRule is a lifecycle configuration rule.
//
// When all the configured conditions are met by an object in the bucket, the
// configured action will automatically be taken on that object.
type LifecycleRule struct {
	// Action is the action to take when all of the associated conditions are
	// met.
	Action LifecycleAction

	// Condition is the set of conditions that must be met for the associated
	// action to be taken.
	Condition LifecycleCondition
}

// LifecycleAction is a lifecycle configuration action.
type LifecycleAction struct {
	// Type is the type of action to take on matching objects.
	//
	// Acceptable values are "Delete" to delete matching objects and
	// "SetStorageClass" to set the storage class defined in StorageClass on
	// matching objects.
	Type string

	// StorageClass is the storage class to set on matching objects if the Action
	// is "SetStorageClass".
	StorageClass string
}

// Liveness specifies whether the object is live or not.
type Liveness int

const (
	// LiveAndArchived includes both live and archived objects.
	LiveAndArchived Liveness = iota
	// Live specifies that the object is still live.
	Live
	// Archived specifies that the object is archived.
	Archived
)

// LifecycleCondition is a set of conditions used to match objects and take an
// action automatically.
//
// All configured conditions must be met for the associated action to be taken.
type LifecycleCondition struct {
	// AgeInDays is the age of the object in days.
	AgeInDays int64

	// CreatedBefore is the time the object was created.
	//
	// This condition is satisfied when an object is created before midnight of
	// the specified date in UTC.
	CreatedBefore time.Time

	// CustomTimeBefore is the CustomTime metadata field of the object. This
	// condition is satisfied when an object's CustomTime timestamp is before
	// midnight of the specified date in UTC.
	//
	// This condition can only be satisfied if CustomTime has been set.
	CustomTimeBefore time.Time

	// DaysSinceCustomTime is the days elapsed since the CustomTime date of the
	// object. This condition can only be satisfied if CustomTime has been set.
	DaysSinceCustomTime int64

	// DaysSinceNoncurrentTime is the days elapsed since the noncurrent timestamp
	// of the object. This condition is relevant only for versioned objects.
	DaysSinceNoncurrentTime int64

	// Liveness specifies the object's liveness. Relevant only for versioned objects
	Liveness Liveness

	// MatchesStorageClasses is the condition matching the object's storage
	// class.
	//
	// Values include "STANDARD", "NEARLINE", "COLDLINE" and "ARCHIVE".
	MatchesStorageClasses []string

	// NoncurrentTimeBefore is the noncurrent timestamp of the object. This
	// condition is satisfied when an object's noncurrent timestamp is before
	// midnight of the specified date in UTC.
	//
	// This condition is relevant only for versioned objects.
	NoncurrentTimeBefore time.Time

	// NumNewerVersions is the condition matching objects with a number of newer versions.
	//
	// If the value is N, this condition is satisfied when there are at least N
	// versions (including the live version) newer than this version of the
	// object.
	NumNewerVersions int64
}

// BucketLogging holds the bucket's logging configuration, which defines the
// destination bucket and optional name prefix for the current bucket's
// logs.
type BucketLogging struct {
	// The destination bucket where the current bucket's logs
	// should be placed.
	LogBucket string

	// A prefix for log object names.
	LogObjectPrefix string
}

// BucketWebsite holds the bucket's website configuration, controlling how the
// service behaves when accessing bucket contents as a web site. See
// https://cloud.google.com/storage/docs/static-website for more information.
type BucketWebsite struct {
	// If the requested object path is missing, the service will ensure the path has
	// a trailing '/', append this suffix, and attempt to retrieve the resulting
	// object. This allows the creation of index.html objects to represent directory
	// pages.
	MainPageSuffix string

	// If the requested object path is missing, and any mainPageSuffix object is
	// missing, if applicable, the service will return the named object from this
	// bucket as the content for a 404 Not Found result.
	NotFoundPage string
}

func newBucket(b *raw.Bucket) (*BucketAttrs, error) {
	if b == nil {
		return nil, nil
	}
	rp, err := toRetentionPolicy(b.RetentionPolicy)
	if err != nil {
		return nil, err
	}
	return &BucketAttrs{
		Name:                     b.Name,
		Location:                 b.Location,
		MetaGeneration:           b.Metageneration,
		DefaultEventBasedHold:    b.DefaultEventBasedHold,
		StorageClass:             b.StorageClass,
		Created:                  convertTime(b.TimeCreated),
		VersioningEnabled:        b.Versioning != nil && b.Versioning.Enabled,
		ACL:                      toBucketACLRules(b.Acl),
		DefaultObjectACL:         toObjectACLRules(b.DefaultObjectAcl),
		Labels:                   b.Labels,
		RequesterPays:            b.Billing != nil && b.Billing.RequesterPays,
		Lifecycle:                toLifecycle(b.Lifecycle),
		RetentionPolicy:          rp,
		CORS:                     toCORS(b.Cors),
		Encryption:               toBucketEncryption(b.Encryption),
		Logging:                  toBucketLogging(b.Logging),
		Website:                  toBucketWebsite(b.Website),
		BucketPolicyOnly:         toBucketPolicyOnly(b.IamConfiguration),
		UniformBucketLevelAccess: toUniformBucketLevelAccess(b.IamConfiguration),
		PublicAccessPrevention:   toPublicAccessPrevention(b.IamConfiguration),
		Etag:                     b.Etag,
		LocationType:             b.LocationType,
		ProjectNumber:            b.ProjectNumber,
		RPO:                      toRPO(b),
	}, nil
}

// toRawBucket copies the editable attribute from b to the raw library's Bucket type.
func (b *BucketAttrs) toRawBucket() *raw.Bucket {
	// Copy label map.
	var labels map[string]string
	if len(b.Labels) > 0 {
		labels = make(map[string]string, len(b.Labels))
		for k, v := range b.Labels {
			labels[k] = v
		}
	}
	// Ignore VersioningEnabled if it is false. This is OK because
	// we only call this method when creating a bucket, and by default
	// new buckets have versioning off.
	var v *raw.BucketVersioning
	if b.VersioningEnabled {
		v = &raw.BucketVersioning{Enabled: true}
	}
	var bb *raw.BucketBilling
	if b.RequesterPays {
		bb = &raw.BucketBilling{RequesterPays: true}
	}
	var bktIAM *raw.BucketIamConfiguration
	if b.UniformBucketLevelAccess.Enabled || b.BucketPolicyOnly.Enabled || b.PublicAccessPrevention != PublicAccessPreventionUnknown {
		bktIAM = &raw.BucketIamConfiguration{}
		if b.UniformBucketLevelAccess.Enabled || b.BucketPolicyOnly.Enabled {
			bktIAM.UniformBucketLevelAccess = &raw.BucketIamConfigurationUniformBucketLevelAccess{
				Enabled: true,
			}
		}
		if b.PublicAccessPrevention != PublicAccessPreventionUnknown {
			bktIAM.PublicAccessPrevention = b.PublicAccessPrevention.String()
		}
	}
	return &raw.Bucket{
		Name:             b.Name,
		Location:         b.Location,
		StorageClass:     b.StorageClass,
		Acl:              toRawBucketACL(b.ACL),
		DefaultObjectAcl: toRawObjectACL(b.DefaultObjectACL),
		Versioning:       v,
		Labels:           labels,
		Billing:          bb,
		Lifecycle:        toRawLifecycle(b.Lifecycle),
		RetentionPolicy:  b.RetentionPolicy.toRawRetentionPolicy(),
		Cors:             toRawCORS(b.CORS),
		Encryption:       b.Encryption.toRawBucketEncryption(),
		Logging:          b.Logging.toRawBucketLogging(),
		Website:          b.Website.toRawBucketWebsite(),
		IamConfiguration: bktIAM,
		Rpo:              b.RPO.String(),
	}
}

// CORS is the bucket's Cross-Origin Resource Sharing (CORS) configuration.
type CORS struct {
	// MaxAge is the value to return in the Access-Control-Max-Age
	// header used in preflight responses.
	MaxAge time.Duration

	// Methods is the list of HTTP methods on which to include CORS response
	// headers, (GET, OPTIONS, POST, etc) Note: "*" is permitted in the list
	// of methods, and means "any method".
	Methods []string

	// Origins is the list of Origins eligible to receive CORS response
	// headers. Note: "*" is permitted in the list of origins, and means
	// "any Origin".
	Origins []string

	// ResponseHeaders is the list of HTTP headers other than the simple
	// response headers to give permission for the user-agent to share
	// across domains.
	ResponseHeaders []string
}

// BucketEncryption is a bucket's encryption configuration.
type BucketEncryption struct {
	// A Cloud KMS key name, in the form
	// projects/P/locations/L/keyRings/R/cryptoKeys/K, that will be used to encrypt
	// objects inserted into this bucket, if no encryption method is specified.
	// The key's location must be the same as the bucket's.
	DefaultKMSKeyName string
}

// BucketAttrsToUpdate define the attributes to update during an Update call.
type BucketAttrsToUpdate struct {
	// If set, updates whether the bucket uses versioning.
	VersioningEnabled optional.Bool

	// If set, updates whether the bucket is a Requester Pays bucket.
	RequesterPays optional.Bool

	// DefaultEventBasedHold is the default value for event-based hold on
	// newly created objects in this bucket.
	DefaultEventBasedHold optional.Bool

	// BucketPolicyOnly is an alias for UniformBucketLevelAccess. Use of
	// UniformBucketLevelAccess is recommended above the use of this field.
	// Setting BucketPolicyOnly.Enabled OR UniformBucketLevelAccess.Enabled to
	// true, will enable UniformBucketLevelAccess. If both BucketPolicyOnly and
	// UniformBucketLevelAccess are set, the value of UniformBucketLevelAccess
	// will take precedence.
	BucketPolicyOnly *BucketPolicyOnly

	// UniformBucketLevelAccess configures access checks to use only bucket-level IAM
	// policies and ignore any ACL rules for the bucket.
	// See https://cloud.google.com/storage/docs/uniform-bucket-level-access
	// for more information.
	UniformBucketLevelAccess *UniformBucketLevelAccess

	// PublicAccessPrevention is the setting for the bucket's
	// PublicAccessPrevention policy, which can be used to prevent public access
	// of data in the bucket. See
	// https://cloud.google.com/storage/docs/public-access-prevention for more
	// information.
	PublicAccessPrevention PublicAccessPrevention

	// StorageClass is the default storage class of the bucket. This defines
	// how objects in the bucket are stored and determines the SLA
	// and the cost of storage. Typical values are "STANDARD", "NEARLINE",
	// "COLDLINE" and "ARCHIVE". Defaults to "STANDARD".
	// See https://cloud.google.com/storage/docs/storage-classes for all
	// valid values.
	StorageClass string

	// If set, updates the retention policy of the bucket. Using
	// RetentionPolicy.RetentionPeriod = 0 will delete the existing policy.
	//
	// This feature is in private alpha release. It is not currently available to
	// most customers. It might be changed in backwards-incompatible ways and is not
	// subject to any SLA or deprecation policy.
	RetentionPolicy *RetentionPolicy

	// If set, replaces the CORS configuration with a new configuration.
	// An empty (rather than nil) slice causes all CORS policies to be removed.
	CORS []CORS

	// If set, replaces the encryption configuration of the bucket. Using
	// BucketEncryption.DefaultKMSKeyName = "" will delete the existing
	// configuration.
	Encryption *BucketEncryption

	// If set, replaces the lifecycle configuration of the bucket.
	Lifecycle *Lifecycle

	// If set, replaces the logging configuration of the bucket.
	Logging *BucketLogging

	// If set, replaces the website configuration of the bucket.
	Website *BucketWebsite

	// If not empty, applies a predefined set of access controls.
	// See https://cloud.google.com/storage/docs/json_api/v1/buckets/patch.
	PredefinedACL string

	// If not empty, applies a predefined set of default object access controls.
	// See https://cloud.google.com/storage/docs/json_api/v1/buckets/patch.
	PredefinedDefaultObjectACL string

	// RPO configures the Recovery Point Objective (RPO) policy of the bucket.
	// Set to RPOAsyncTurbo to turn on Turbo Replication for a bucket.
	// See https://cloud.google.com/storage/docs/managing-turbo-replication for
	// more information.
	RPO RPO

	setLabels    map[string]string
	deleteLabels map[string]bool
}

// SetLabel causes a label to be added or modified when ua is used
// in a call to Bucket.Update.
func (ua *BucketAttrsToUpdate) SetLabel(name, value string) {
	if ua.setLabels == nil {
		ua.setLabels = map[string]string{}
	}
	ua.setLabels[name] = value
}

// DeleteLabel causes a label to be deleted when ua is used in a
// call to Bucket.Update.
func (ua *BucketAttrsToUpdate) DeleteLabel(name string) {
	if ua.deleteLabels == nil {
		ua.deleteLabels = map[string]bool{}
	}
	ua.deleteLabels[name] = true
}

func (ua *BucketAttrsToUpdate) toRawBucket() *raw.Bucket {
	rb := &raw.Bucket{}
	if ua.CORS != nil {
		rb.Cors = toRawCORS(ua.CORS)
		rb.ForceSendFields = append(rb.ForceSendFields, "Cors")
	}
	if ua.DefaultEventBasedHold != nil {
		rb.DefaultEventBasedHold = optional.ToBool(ua.DefaultEventBasedHold)
		rb.ForceSendFields = append(rb.ForceSendFields, "DefaultEventBasedHold")
	}
	if ua.RetentionPolicy != nil {
		if ua.RetentionPolicy.RetentionPeriod == 0 {
			rb.NullFields = append(rb.NullFields, "RetentionPolicy")
			rb.RetentionPolicy = nil
		} else {
			rb.RetentionPolicy = ua.RetentionPolicy.toRawRetentionPolicy()
		}
	}
	if ua.VersioningEnabled != nil {
		rb.Versioning = &raw.BucketVersioning{
			Enabled:         optional.ToBool(ua.VersioningEnabled),
			ForceSendFields: []string{"Enabled"},
		}
	}
	if ua.RequesterPays != nil {
		rb.Billing = &raw.BucketBilling{
			RequesterPays:   optional.ToBool(ua.RequesterPays),
			ForceSendFields: []string{"RequesterPays"},
		}
	}
	if ua.BucketPolicyOnly != nil {
		rb.IamConfiguration = &raw.BucketIamConfiguration{
			UniformBucketLevelAccess: &raw.BucketIamConfigurationUniformBucketLevelAccess{
				Enabled:         ua.BucketPolicyOnly.Enabled,
				ForceSendFields: []string{"Enabled"},
			},
		}
	}
	if ua.UniformBucketLevelAccess != nil {
		rb.IamConfiguration = &raw.BucketIamConfiguration{
			UniformBucketLevelAccess: &raw.BucketIamConfigurationUniformBucketLevelAccess{
				Enabled:         ua.UniformBucketLevelAccess.Enabled,
				ForceSendFields: []string{"Enabled"},
			},
		}
	}
	if ua.PublicAccessPrevention != PublicAccessPreventionUnknown {
		if rb.IamConfiguration == nil {
			rb.IamConfiguration = &raw.BucketIamConfiguration{}
		}
		rb.IamConfiguration.PublicAccessPrevention = ua.PublicAccessPrevention.String()
	}
	if ua.Encryption != nil {
		if ua.Encryption.DefaultKMSKeyName == "" {
			rb.NullFields = append(rb.NullFields, "Encryption")
			rb.Encryption = nil
		} else {
			rb.Encryption = ua.Encryption.toRawBucketEncryption()
		}
	}
	if ua.Lifecycle != nil {
		rb.Lifecycle = toRawLifecycle(*ua.Lifecycle)
		rb.ForceSendFields = append(rb.ForceSendFields, "Lifecycle")
	}
	if ua.Logging != nil {
		if *ua.Logging == (BucketLogging{}) {
			rb.NullFields = append(rb.NullFields, "Logging")
			rb.Logging = nil
		} else {
			rb.Logging = ua.Logging.toRawBucketLogging()
		}
	}
	if ua.Website != nil {
		if *ua.Website == (BucketWebsite{}) {
			rb.NullFields = append(rb.NullFields, "Website")
			rb.Website = nil
		} else {
			rb.Website = ua.Website.toRawBucketWebsite()
		}
	}
	if ua.PredefinedACL != "" {
		// Clear ACL or the call will fail.
		rb.Acl = nil
		rb.ForceSendFields = append(rb.ForceSendFields, "Acl")
	}
	if ua.PredefinedDefaultObjectACL != "" {
		// Clear ACLs or the call will fail.
		rb.DefaultObjectAcl = nil
		rb.ForceSendFields = append(rb.ForceSendFields, "DefaultObjectAcl")
	}

	rb.StorageClass = ua.StorageClass
	rb.Rpo = ua.RPO.String()

	if ua.setLabels != nil || ua.deleteLabels != nil {
		rb.Labels = map[string]string{}
		for k, v := range ua.setLabels {
			rb.Labels[k] = v
		}
		if len(rb.Labels) == 0 && len(ua.deleteLabels) > 0 {
			rb.ForceSendFields = append(rb.ForceSendFields, "Labels")
		}
		for l := range ua.deleteLabels {
			rb.NullFields = append(rb.NullFields, "Labels."+l)
		}
	}
	return rb
}

// If returns a new BucketHandle that applies a set of preconditions.
// Preconditions already set on the BucketHandle are ignored.
// Operations on the new handle will return an error if the preconditions are not
// satisfied. The only valid preconditions for buckets are MetagenerationMatch
// and MetagenerationNotMatch.
func (b *BucketHandle) If(conds BucketConditions) *BucketHandle {
	b2 := *b
	b2.conds = &conds
	return &b2
}

// BucketConditions constrain bucket methods to act on specific metagenerations.
//
// The zero value is an empty set of constraints.
type BucketConditions struct {
	// MetagenerationMatch specifies that the bucket must have the given
	// metageneration for the operation to occur.
	// If MetagenerationMatch is zero, it has no effect.
	MetagenerationMatch int64

	// MetagenerationNotMatch specifies that the bucket must not have the given
	// metageneration for the operation to occur.
	// If MetagenerationNotMatch is zero, it has no effect.
	MetagenerationNotMatch int64
}

func (c *BucketConditions) validate(method string) error {
	if *c == (BucketConditions{}) {
		return fmt.Errorf("storage: %s: empty conditions", method)
	}
	if c.MetagenerationMatch != 0 && c.MetagenerationNotMatch != 0 {
		return fmt.Errorf("storage: %s: multiple conditions specified for metageneration", method)
	}
	return nil
}

// UserProject returns a new BucketHandle that passes the project ID as the user
// project for all subsequent calls. Calls with a user project will be billed to that
// project rather than to the bucket's owning project.
//
// A user project is required for all operations on Requester Pays buckets.
func (b *BucketHandle) UserProject(projectID string) *BucketHandle {
	b2 := *b
	b2.userProject = projectID
	b2.acl.userProject = projectID
	b2.defaultObjectACL.userProject = projectID
	return &b2
}

// LockRetentionPolicy locks a bucket's retention policy until a previously-configured
// RetentionPeriod past the EffectiveTime. Note that if RetentionPeriod is set to less
// than a day, the retention policy is treated as a development configuration and locking
// will have no effect. The BucketHandle must have a metageneration condition that
// matches the bucket's metageneration. See BucketHandle.If.
//
// This feature is in private alpha release. It is not currently available to
// most customers. It might be changed in backwards-incompatible ways and is not
// subject to any SLA or deprecation policy.
func (b *BucketHandle) LockRetentionPolicy(ctx context.Context) error {
	var metageneration int64
	if b.conds != nil {
		metageneration = b.conds.MetagenerationMatch
	}
	req := b.c.raw.Buckets.LockRetentionPolicy(b.name, metageneration)
	return run(ctx, func() error {
		_, err := req.Context(ctx).Do()
		return err
	}, b.retry, true)
}

// applyBucketConds modifies the provided call using the conditions in conds.
// call is something that quacks like a *raw.WhateverCall.
func applyBucketConds(method string, conds *BucketConditions, call interface{}) error {
	if conds == nil {
		return nil
	}
	if err := conds.validate(method); err != nil {
		return err
	}
	cval := reflect.ValueOf(call)
	switch {
	case conds.MetagenerationMatch != 0:
		if !setConditionField(cval, "IfMetagenerationMatch", conds.MetagenerationMatch) {
			return fmt.Errorf("storage: %s: ifMetagenerationMatch not supported", method)
		}
	case conds.MetagenerationNotMatch != 0:
		if !setConditionField(cval, "IfMetagenerationNotMatch", conds.MetagenerationNotMatch) {
			return fmt.Errorf("storage: %s: ifMetagenerationNotMatch not supported", method)
		}
	}
	return nil
}

func (rp *RetentionPolicy) toRawRetentionPolicy() *raw.BucketRetentionPolicy {
	if rp == nil {
		return nil
	}
	return &raw.BucketRetentionPolicy{
		RetentionPeriod: int64(rp.RetentionPeriod / time.Second),
	}
}

func toRetentionPolicy(rp *raw.BucketRetentionPolicy) (*RetentionPolicy, error) {
	if rp == nil {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, rp.EffectiveTime)
	if err != nil {
		return nil, err
	}
	return &RetentionPolicy{
		RetentionPeriod: time.Duration(rp.RetentionPeriod) * time.Second,
		EffectiveTime:   t,
		IsLocked:        rp.IsLocked,
	}, nil
}

func toRawCORS(c []CORS) []*raw.BucketCors {
	var out []*raw.BucketCors
	for _, v := range c {
		out = append(out, &raw.BucketCors{
			MaxAgeSeconds:  int64(v.MaxAge / time.Second),
			Method:         v.Methods,
			Origin:         v.Origins,
			ResponseHeader: v.ResponseHeaders,
		})
	}
	return out
}

func toCORS(rc []*raw.BucketCors) []CORS {
	var out []CORS
	for _, v := range rc {
		out = append(out, CORS{
			MaxAge:          time.Duration(v.MaxAgeSeconds) * time.Second,
			Methods:         v.Method,
			Origins:         v.Origin,
			ResponseHeaders: v.ResponseHeader,
		})
	}
	return out
}

func toRawLifecycle(l Lifecycle) *raw.BucketLifecycle {
	var rl raw.BucketLifecycle
	if len(l.Rules) == 0 {
		rl.ForceSendFields = []string{"Rule"}
	}
	for _, r := range l.Rules {
		rr := &raw.BucketLifecycleRule{
			Action: &raw.BucketLifecycleRuleAction{
				Type:         r.Action.Type,
				StorageClass: r.Action.StorageClass,
			},
			Condition: &raw.BucketLifecycleRuleCondition{
				Age:                     r.Condition.AgeInDays,
				DaysSinceCustomTime:     r.Condition.DaysSinceCustomTime,
				DaysSinceNoncurrentTime: r.Condition.DaysSinceNoncurrentTime,
				MatchesStorageClass:     r.Condition.MatchesStorageClasses,
				NumNewerVersions:        r.Condition.NumNewerVersions,
			},
		}

		switch r.Condition.Liveness {
		case LiveAndArchived:
			rr.Condition.IsLive = nil
		case Live:
			rr.Condition.IsLive = googleapi.Bool(true)
		case Archived:
			rr.Condition.IsLive = googleapi.Bool(false)
		}

		if !r.Condition.CreatedBefore.IsZero() {
			rr.Condition.CreatedBefore = r.Condition.CreatedBefore.Format(rfc3339Date)
		}
		if !r.Condition.CustomTimeBefore.IsZero() {
			rr.Condition.CustomTimeBefore = r.Condition.CustomTimeBefore.Format(rfc3339Date)
		}
		if !r.Condition.NoncurrentTimeBefore.IsZero() {
			rr.Condition.NoncurrentTimeBefore = r.Condition.NoncurrentTimeBefore.Format(rfc3339Date)
		}
		rl.Rule = append(rl.Rule, rr)
	}
	return &rl
}

func toLifecycle(rl *raw.BucketLifecycle) Lifecycle {
	var l Lifecycle
	if rl == nil {
		return l
	}
	for _, rr := range rl.Rule {
		r := LifecycleRule{
			Action: LifecycleAction{
				Type:         rr.Action.Type,
				StorageClass: rr.Action.StorageClass,
			},
			Condition: LifecycleCondition{
				AgeInDays:               rr.Condition.Age,
				DaysSinceCustomTime:     rr.Condition.DaysSinceCustomTime,
				DaysSinceNoncurrentTime: rr.Condition.DaysSinceNoncurrentTime,
				MatchesStorageClasses:   rr.Condition.MatchesStorageClass,
				NumNewerVersions:        rr.Condition.NumNewerVersions,
			},
		}

		if rr.Condition.IsLive == nil {
			r.Condition.Liveness = LiveAndArchived
		} else if *rr.Condition.IsLive {
			r.Condition.Liveness = Live
		} else {
			r.Condition.Liveness = Archived
		}

		if rr.Condition.CreatedBefore != "" {
			r.Condition.CreatedBefore, _ = time.Parse(rfc3339Date, rr.Condition.CreatedBefore)
		}
		if rr.Condition.CustomTimeBefore != "" {
			r.Condition.CustomTimeBefore, _ = time.Parse(rfc3339Date, rr.Condition.CustomTimeBefore)
		}
		if rr.Condition.NoncurrentTimeBefore != "" {
			r.Condition.NoncurrentTimeBefore, _ = time.Parse(rfc3339Date, rr.Condition.NoncurrentTimeBefore)
		}
		l.Rules = append(l.Rules, r)
	}
	return l
}

func (e *BucketEncryption) toRawBucketEncryption() *raw.BucketEncryption {
	if e == nil {
		return nil
	}
	return &raw.BucketEncryption{
		DefaultKmsKeyName: e.DefaultKMSKeyName,
	}
}

func toBucketEncryption(e *raw.BucketEncryption) *BucketEncryption {
	if e == nil {
		return nil
	}
	return &BucketEncryption{DefaultKMSKeyName: e.DefaultKmsKeyName}
}

func (b *BucketLogging) toRawBucketLogging() *raw.BucketLogging {
	if b == nil {
		return nil
	}
	return &raw.BucketLogging{
		LogBucket:       b.LogBucket,
		LogObjectPrefix: b.LogObjectPrefix,
	}
}

func toBucketLogging(b *raw.BucketLogging) *BucketLogging {
	if b == nil {
		return nil
	}
	return &BucketLogging{
		LogBucket:       b.LogBucket,
		LogObjectPrefix: b.LogObjectPrefix,
	}
}

func (w *BucketWebsite) toRawBucketWebsite() *raw.BucketWebsite {
	if w == nil {
		return nil
	}
	return &raw.BucketWebsite{
		MainPageSuffix: w.MainPageSuffix,
		NotFoundPage:   w.NotFoundPage,
	}
}

func toBucketWebsite(w *raw.BucketWebsite) *BucketWebsite {
	if w == nil {
		return nil
	}
	return &BucketWebsite{
		MainPageSuffix: w.MainPageSuffix,
		NotFoundPage:   w.NotFoundPage,
	}
}

func toBucketPolicyOnly(b *raw.BucketIamConfiguration) BucketPolicyOnly {
	if b == nil || b.BucketPolicyOnly == nil || !b.BucketPolicyOnly.Enabled {
		return BucketPolicyOnly{}
	}
	lt, err := time.Parse(time.RFC3339, b.BucketPolicyOnly.LockedTime)
	if err != nil {
		return BucketPolicyOnly{
			Enabled: true,
		}
	}
	return BucketPolicyOnly{
		Enabled:    true,
		LockedTime: lt,
	}
}

func toUniformBucketLevelAccess(b *raw.BucketIamConfiguration) UniformBucketLevelAccess {
	if b == nil || b.UniformBucketLevelAccess == nil || !b.UniformBucketLevelAccess.Enabled {
		return UniformBucketLevelAccess{}
	}
	lt, err := time.Parse(time.RFC3339, b.UniformBucketLevelAccess.LockedTime)
	if err != nil {
		return UniformBucketLevelAccess{
			Enabled: true,
		}
	}
	return UniformBucketLevelAccess{
		Enabled:    true,
		LockedTime: lt,
	}
}

func toPublicAccessPrevention(b *raw.BucketIamConfiguration) PublicAccessPrevention {
	if b == nil {
		return PublicAccessPreventionUnknown
	}
	switch b.PublicAccessPrevention {
	case publicAccessPreventionInherited, publicAccessPreventionUnspecified:
		return PublicAccessPreventionInherited
	case publicAccessPreventionEnforced:
		return PublicAccessPreventionEnforced
	default:
		return PublicAccessPreventionUnknown
	}
}

func toRPO(b *raw.Bucket) RPO {
	if b == nil {
		return RPOUnknown
	}
	switch b.Rpo {
	case rpoDefault:
		return RPODefault
	case rpoAsyncTurbo:
		return RPOAsyncTurbo
	default:
		return RPOUnknown
	}
}

// Objects returns an iterator over the objects in the bucket that match the
// Query q. If q is nil, no filtering is done. Objects will be iterated over
// lexicographically by name.
//
// Note: The returned iterator is not safe for concurrent operations without explicit synchronization.
func (b *BucketHandle) Objects(ctx context.Context, q *Query) *ObjectIterator {
	it := &ObjectIterator{
		ctx:    ctx,
		bucket: b,
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.fetch,
		func() int { return len(it.items) },
		func() interface{} { b := it.items; it.items = nil; return b })
	if q != nil {
		it.query = *q
	}
	return it
}

// Retryer returns a bucket handle that is configured with custom retry
// behavior as specified by the options that are passed to it. All operations
// on the new handle will use the customized retry configuration.
// Retry options set on a object handle will take precedence over options set on
// the bucket handle.
// These retry options will merge with the client's retry configuration (if set)
// for the returned handle. Options passed into this method will take precedence
// over retry options on the client. Note that you must explicitly pass in each
// option you want to override.
func (b *BucketHandle) Retryer(opts ...RetryOption) *BucketHandle {
	b2 := *b
	var retry *retryConfig
	if b.retry != nil {
		// merge the options with the existing retry
		retry = b.retry
	} else {
		retry = &retryConfig{}
	}
	for _, opt := range opts {
		opt.apply(retry)
	}
	b2.retry = retry
	b2.acl.retry = retry
	b2.defaultObjectACL.retry = retry
	return &b2
}

// An ObjectIterator is an iterator over ObjectAttrs.
//
// Note: This iterator is not safe for concurrent operations without explicit synchronization.
type ObjectIterator struct {
	ctx      context.Context
	bucket   *BucketHandle
	query    Query
	pageInfo *iterator.PageInfo
	nextFunc func() error
	items    []*ObjectAttrs
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
//
// Note: This method is not safe for concurrent operations without explicit synchronization.
func (it *ObjectIterator) PageInfo() *iterator.PageInfo { return it.pageInfo }

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns iterator.Done, all subsequent
// calls will return iterator.Done.
//
// In addition, if Next returns an error other than iterator.Done, all
// subsequent calls will return the same error. To continue iteration, a new
// `ObjectIterator` must be created. Since objects are ordered lexicographically
// by name, `Query.StartOffset` can be used to create a new iterator which will
// start at the desired place. See
// https://pkg.go.dev/cloud.google.com/go/storage?tab=doc#hdr-Listing_objects.
//
// If Query.Delimiter is non-empty, some of the ObjectAttrs returned by Next will
// have a non-empty Prefix field, and a zero value for all other fields. These
// represent prefixes.
//
// Note: This method is not safe for concurrent operations without explicit synchronization.
func (it *ObjectIterator) Next() (*ObjectAttrs, error) {
	if err := it.nextFunc(); err != nil {
		return nil, err
	}
	item := it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ObjectIterator) fetch(pageSize int, pageToken string) (string, error) {
	req := it.bucket.c.raw.Objects.List(it.bucket.name)
	setClientHeader(req.Header())
	projection := it.query.Projection
	if projection == ProjectionDefault {
		projection = ProjectionFull
	}
	req.Projection(projection.String())
	req.Delimiter(it.query.Delimiter)
	req.Prefix(it.query.Prefix)
	req.StartOffset(it.query.StartOffset)
	req.EndOffset(it.query.EndOffset)
	req.Versions(it.query.Versions)
	if len(it.query.fieldSelection) > 0 {
		req.Fields("nextPageToken", googleapi.Field(it.query.fieldSelection))
	}
	req.PageToken(pageToken)
	if it.bucket.userProject != "" {
		req.UserProject(it.bucket.userProject)
	}
	if pageSize > 0 {
		req.MaxResults(int64(pageSize))
	}
	var resp *raw.Objects
	var err error
	err = run(it.ctx, func() error {
		resp, err = req.Context(it.ctx).Do()
		return err
	}, it.bucket.retry, true)
	if err != nil {
		var e *googleapi.Error
		if ok := xerrors.As(err, &e); ok && e.Code == http.StatusNotFound {
			err = ErrBucketNotExist
		}
		return "", err
	}
	for _, item := range resp.Items {
		it.items = append(it.items, newObject(item))
	}
	for _, prefix := range resp.Prefixes {
		it.items = append(it.items, &ObjectAttrs{Prefix: prefix})
	}
	return resp.NextPageToken, nil
}

// Buckets returns an iterator over the buckets in the project. You may
// optionally set the iterator's Prefix field to restrict the list to buckets
// whose names begin with the prefix. By default, all buckets in the project
// are returned.
//
// Note: The returned iterator is not safe for concurrent operations without explicit synchronization.
func (c *Client) Buckets(ctx context.Context, projectID string) *BucketIterator {
	it := &BucketIterator{
		ctx:       ctx,
		client:    c,
		projectID: projectID,
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.fetch,
		func() int { return len(it.buckets) },
		func() interface{} { b := it.buckets; it.buckets = nil; return b })

	return it
}

// A BucketIterator is an iterator over BucketAttrs.
//
// Note: This iterator is not safe for concurrent operations without explicit synchronization.
type BucketIterator struct {
	// Prefix restricts the iterator to buckets whose names begin with it.
	Prefix string

	ctx       context.Context
	client    *Client
	projectID string
	buckets   []*BucketAttrs
	pageInfo  *iterator.PageInfo
	nextFunc  func() error
}

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns iterator.Done, all subsequent
// calls will return iterator.Done.
//
// Note: This method is not safe for concurrent operations without explicit synchronization.
func (it *BucketIterator) Next() (*BucketAttrs, error) {
	if err := it.nextFunc(); err != nil {
		return nil, err
	}
	b := it.buckets[0]
	it.buckets = it.buckets[1:]
	return b, nil
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
//
// Note: This method is not safe for concurrent operations without explicit synchronization.
func (it *BucketIterator) PageInfo() *iterator.PageInfo { return it.pageInfo }

func (it *BucketIterator) fetch(pageSize int, pageToken string) (token string, err error) {
	req := it.client.raw.Buckets.List(it.projectID)
	setClientHeader(req.Header())
	req.Projection("full")
	req.Prefix(it.Prefix)
	req.PageToken(pageToken)
	if pageSize > 0 {
		req.MaxResults(int64(pageSize))
	}
	var resp *raw.Buckets
	err = run(it.ctx, func() error {
		resp, err = req.Context(it.ctx).Do()
		return err
	}, it.client.retry, true)
	if err != nil {
		return "", err
	}
	for _, item := range resp.Items {
		b, err := newBucket(item)
		if err != nil {
			return "", err
		}
		it.buckets = append(it.buckets, b)
	}
	return resp.NextPageToken, nil
}

// RPO (Recovery Point Objective) configures the turbo replication feature. See
// https://cloud.google.com/storage/docs/managing-turbo-replication for more information.
type RPO int

const (
	// RPOUnknown is a zero value. It may be returned from bucket.Attrs() if RPO
	// is not present in the bucket metadata, that is, the bucket is not dual-region.
	// This value is also used if the RPO field is not set in a call to GCS.
	RPOUnknown RPO = iota

	// RPODefault represents default replication. It is used to reset RPO on an
	// existing bucket  that has this field set to RPOAsyncTurbo. Otherwise it
	// is equivalent to RPOUnknown, and is always ignored. This value is valid
	// for dual- or multi-region buckets.
	RPODefault

	// RPOAsyncTurbo represents turbo replication and is used to enable Turbo
	// Replication on a bucket. This value is only valid for dual-region buckets.
	RPOAsyncTurbo

	rpoUnknown    string = ""
	rpoDefault           = "DEFAULT"
	rpoAsyncTurbo        = "ASYNC_TURBO"
)

func (rpo RPO) String() string {
	switch rpo {
	case RPODefault:
		return rpoDefault
	case RPOAsyncTurbo:
		return rpoAsyncTurbo
	default:
		return rpoUnknown
	}
}
