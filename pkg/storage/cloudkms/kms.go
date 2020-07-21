// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cloudkms

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
)

const KMSRegionParam = "REGION"

// KMS provides an API to interact with a KMS service.
type KMS interface {
	ID() (string error)
	Conf() roachpb.KMS
	Encrypt(data []byte) ([]byte, error)
	Decrypt(data []byte) ([]byte, error)
}

// KMSEnv is the environment in which a KMS is configured and used.
type KMSEnv interface {
	ClusterSettings() *cluster.Settings
	KMSConfig() *base.KMSConfig
}

// KMSFromURIFactory describes a factory function for KMS given a URI.
type KMSFromURIFactory func(ctx context.Context, uri string, env KMSEnv) (KMS, error)

// Mapping from KMS scheme to its registered factory method.
var kmsFactoryMap = make(map[string]KMSFromURIFactory)

// RegisterKMSFromURIFactory is used by every concrete KMS implementation to
// register its factory method.
func RegisterKMSFromURIFactory(factory KMSFromURIFactory, scheme string) {
	kmsFactoryMap[scheme] = factory
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
