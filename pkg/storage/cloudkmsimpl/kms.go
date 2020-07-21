// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cloudkmsimpl

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudkms"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
)

func kmsConfFromURI(uri string) (roachpb.KMS, error) {
	conf := roachpb.KMS{}
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return conf, err
	}

	switch kmsURI.Scheme {
	case awsScheme:
		conf.Provider = roachpb.KMSProvider_AWSKMS
		conf.AWSKMSConfig = &roachpb.KMS_AWSKMS{
			KeyID:     strings.TrimPrefix(kmsURI.Path, "/"),
			AccessKey: kmsURI.Query().Get(cloud.S3AccessKeyParam),
			Secret:    kmsURI.Query().Get(cloud.S3SecretParam),
			TempToken: kmsURI.Query().Get(cloud.S3TempTokenParam),
			Endpoint:  kmsURI.Query().Get(cloud.S3EndpointParam),
			Region:    kmsURI.Query().Get(cloudkms.KMSRegionParam),
			Auth:      kmsURI.Query().Get(cloud.AuthParam),
		}

		// AWS secrets often contain + characters, which must be escaped when
		// included in a query string; otherwise, they represent a space character.
		// More than a few users have been bitten by this.
		//
		// Luckily, AWS secrets are base64-encoded data and thus will never actually
		// contain spaces. We can convert any space characters we see to +
		// characters to recover the original secret.
		conf.AWSKMSConfig.Secret = strings.Replace(conf.AWSKMSConfig.Secret, " ", "+", -1)
	default:
		return conf, errors.Newf("unsupported KMS scheme %s", kmsURI.Scheme)
	}

	return conf, nil
}
