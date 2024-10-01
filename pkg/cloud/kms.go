// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

// KMS provides an API to interact with a KMS service.
type KMS interface {
	// MasterKeyID will return the identifier used to reference the master key
	// associated with the KMS object.
	MasterKeyID() string
	// Encrypt returns the ciphertext version of data after encrypting it using
	// the KMS.
	Encrypt(ctx context.Context, data []byte) ([]byte, error)
	// Decrypt returns the plaintext version of data after decrypting it using the
	// KMS.
	Decrypt(ctx context.Context, data []byte) ([]byte, error)
	// Close may be used to perform the necessary cleanup and shutdown of the
	// KMS connection.
	Close() error
}

// KMSEnv is the environment in which a KMS is configured and used.
type KMSEnv interface {
	ClusterSettings() *cluster.Settings
	KMSConfig() *base.ExternalIODirConfig
	DBHandle() isql.DB
	User() username.SQLUsername
}

// KMSFromURIFactory describes a factory function for KMS given a URI.
type KMSFromURIFactory func(ctx context.Context, uri string, env KMSEnv) (KMS, error)

// Mapping from KMS scheme to its registered factory method.
var kmsFactoryMap = make(map[string]KMSFromURIFactory)

var errKMSInaccessible = errors.New("kms inaccessible")

func KMSInaccessible(err error) error {
	return errors.Mark(err, errKMSInaccessible)
}

func IsKMSInaccessible(err error) bool {
	return errors.Is(err, errKMSInaccessible)
}

// RegisterKMSFromURIFactory is used by every concrete KMS implementation to
// register its factory method.
func RegisterKMSFromURIFactory(factory KMSFromURIFactory, schemes ...string) {
	for _, scheme := range schemes {
		if _, ok := kmsFactoryMap[scheme]; ok {
			panic("factory method for " + scheme + " has already been registered")
		}
		kmsFactoryMap[scheme] = factory
	}
}

// KMSFromURI is the method used to create a KMS instance from the provided URI.
func KMSFromURI(ctx context.Context, uri string, env KMSEnv) (KMS, error) {
	var kmsURL *url.URL
	var err error
	if kmsURL, err = url.ParseRequestURI(uri); err != nil {
		return nil, err
	}

	// Find the factory method for the given KMS scheme.
	var factory KMSFromURIFactory
	var ok bool
	if factory, ok = kmsFactoryMap[kmsURL.Scheme]; !ok {
		return nil, errors.Newf("no factory method found for scheme %s", kmsURL.Scheme)
	}

	return factory(ctx, uri, env)
}
