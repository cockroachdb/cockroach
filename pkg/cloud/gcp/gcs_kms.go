// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcp

import (
	"context"
	"encoding/base64"
	"hash/crc32"
	"net/url"
	"strings"

	kms "cloud.google.com/go/kms/apiv1"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const gcsScheme = "gs"

type gcsKMS struct {
	kms                 *kms.KeyManagementClient
	customerMasterKeyID string
}

var _ cloud.KMS = &gcsKMS{}

func init() {
	cloud.RegisterKMSFromURIFactory(MakeGCSKMS, gcsScheme)
}

type kmsURIParams struct {
	credentials string
	auth        string
}

func resolveKMSURIParams(kmsURI url.URL) kmsURIParams {
	params := kmsURIParams{
		credentials: kmsURI.Query().Get(CredentialsParam),
		auth:        kmsURI.Query().Get(cloud.AuthParam),
	}

	return params
}

// MakeGCSKMS is the factory method which returns a configured, ready-to-use
// GCS KMS object.
func MakeGCSKMS(uri string, env cloud.KMSEnv) (cloud.KMS, error) {
	if env.KMSConfig().DisableOutbound {
		return nil, errors.New("external IO must be enabled to use GCS KMS")
	}
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}

	// Extract the URI parameters required to setup the GCS KMS session.
	kmsURIParams := resolveKMSURIParams(*kmsURI)

	// Client options to authenticate and start a GCS KMS session.
	// Currently only accepting json of service account.
	var credentialsOpt []option.ClientOption

	switch kmsURIParams.auth {
	case "", cloud.AuthParamSpecified:
		if kmsURIParams.credentials == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				CredentialsParam,
			)
		}

		// Credentials are passed in base64 encoded, so decode the credentials first.
		credentialsJSON, err := base64.StdEncoding.DecodeString(kmsURIParams.credentials)
		if err != nil {
			return nil, err
		}

		credentialsOpt = append(credentialsOpt, option.WithCredentialsJSON(credentialsJSON))
	case cloud.AuthParamImplicit:
		if env.KMSConfig().DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for gcs due to --external-io-implicit-credentials flag")
		}
		// If implicit credentials used, no client options needed.
	default:
		return nil, errors.Errorf("unsupported value %s for %s", kmsURIParams.auth, cloud.AuthParam)
	}

	ctx := context.Background()

	kmc, err := kms.NewKeyManagementClient(ctx, credentialsOpt...)

	if err != nil {
		return nil, err
	}

	// Remove the key version from the cmk if it's present.
	// https://cloud.google.com/sdk/gcloud/reference/kms/decrypt
	// - For symmetric keys, Cloud KMS detects the decryption key version from the ciphertext.
	//   If you specify a key version as part of a symmetric decryption request,
	//   an error is logged and decryption fails.
	cmkID := strings.Split(kmsURI.Path, "/cryptoKeyVersions/")[0]

	return &gcsKMS{
		kms:                 kmc,
		customerMasterKeyID: strings.TrimPrefix(cmkID, "/"),
	}, nil
}

// MasterKeyID implements the KMS interface.
func (k *gcsKMS) MasterKeyID() (string, error) {
	return k.customerMasterKeyID, nil
}

// Encrypt implements the KMS interface.
func (k *gcsKMS) Encrypt(ctx context.Context, data []byte) ([]byte, error) {
	// Optional but recommended by GCS.
	crc32c := func(data []byte) uint32 {
		t := crc32.MakeTable(crc32.Castagnoli)
		return crc32.Checksum(data, t)
	}
	plaintextCRC32C := crc32c(data)

	encryptInput := &kmspb.EncryptRequest{
		Name:            k.customerMasterKeyID,
		Plaintext:       data,
		PlaintextCrc32C: wrapperspb.Int64(int64(plaintextCRC32C)),
	}

	encryptOutput, err := k.kms.Encrypt(ctx, encryptInput)
	if err != nil {
		return nil, err
	}

	// Optional, but recommended by GCS.
	// For more details on ensuring E2E in-transit integrity to and from Cloud KMS visit:
	// https://cloud.google.com/kms/docs/data-integrity-guidelines
	// TODO(darrylwong): Look into adding some exponential backoff retry behaviour if error is frequent
	if !encryptOutput.VerifiedPlaintextCrc32C {
		return nil, errors.Errorf("Encrypt: request corrupted in-transit")
	}
	if int64(crc32c(encryptOutput.Ciphertext)) != encryptOutput.CiphertextCrc32C.Value {
		return nil, errors.Errorf("Encrypt: response corrupted in-transit")
	}

	return encryptOutput.Ciphertext, nil
}

// Decrypt implements the KMS interface.
func (k *gcsKMS) Decrypt(ctx context.Context, data []byte) ([]byte, error) {
	// Optional but recommended by the documentation
	crc32c := func(data []byte) uint32 {
		t := crc32.MakeTable(crc32.Castagnoli)
		return crc32.Checksum(data, t)
	}
	ciphertextCRC32C := crc32c(data)

	decryptInput := &kmspb.DecryptRequest{
		Name:             k.customerMasterKeyID,
		Ciphertext:       data,
		CiphertextCrc32C: wrapperspb.Int64(int64(ciphertextCRC32C)),
	}

	decryptOutput, err := k.kms.Decrypt(ctx, decryptInput)
	if err != nil {
		return nil, err
	}

	// Optional, but recommended: perform integrity verification on result.
	// For more details on ensuring E2E in-transit integrity to and from Cloud KMS visit:
	// https://cloud.google.com/kms/docs/data-integrity-guidelines
	// TODO(darrylwong): Look into adding some exponential backoff retry behaviour if error is frequent
	if int64(crc32c(decryptOutput.Plaintext)) != decryptOutput.PlaintextCrc32C.Value {
		return nil, errors.Errorf("Decrypt: response corrupted in-transit")
	}

	return decryptOutput.Plaintext, nil
}

// Close implements the KMS interface.
func (k *gcsKMS) Close() error {
	return k.kms.Close()
}
