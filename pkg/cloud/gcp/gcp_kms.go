// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcp

import (
	"context"
	"hash/crc32"
	"net/url"
	"strings"

	kms "cloud.google.com/go/kms/apiv1"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	gcsScheme = "gs"
	gcpScheme = "gcp-kms"
)

type gcsKMS struct {
	kms                 *kms.KeyManagementClient
	customerMasterKeyID string
}

var _ cloud.KMS = &gcsKMS{}

func init() {
	cloud.RegisterKMSFromURIFactory(MakeGCSKMS, gcsScheme, gcpScheme)
}

type kmsURIParams struct {
	credentials   string
	auth          string
	assumeRole    string
	delegateRoles []string
	bearerToken   string
}

// resolveKMSURIParams parses the `kmsURI` for all the supported KMS parameters.
func resolveKMSURIParams(kmsURI cloud.ConsumeURL) (kmsURIParams, error) {
	assumeRole, delegateRoles := cloud.ParseRoleString(kmsURI.ConsumeParam(AssumeRoleParam))
	params := kmsURIParams{
		credentials:   kmsURI.ConsumeParam(CredentialsParam),
		auth:          kmsURI.ConsumeParam(cloud.AuthParam),
		assumeRole:    assumeRole,
		delegateRoles: delegateRoles,
		bearerToken:   kmsURI.ConsumeParam(BearerTokenParam),
	}

	// Validate that all the passed in parameters are supported.
	if unknownParams := kmsURI.RemainingQueryParams(); len(unknownParams) > 0 {
		return kmsURIParams{}, errors.Errorf(
			`unknown KMS query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return params, nil
}

// MakeGCSKMS is the factory method which returns a configured, ready-to-use
// GCS KMS object.
func MakeGCSKMS(ctx context.Context, uri string, env cloud.KMSEnv) (cloud.KMS, error) {
	if env.KMSConfig().DisableOutbound {
		return nil, errors.New("external IO must be enabled to use GCS KMS")
	}
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}
	if kmsURI.Path == "/" {
		return nil, errors.Newf("path component of the KMS cannot be empty; must contain the Customer Managed Key")
	}

	kmsConsumeURL := cloud.ConsumeURL{URL: kmsURI}
	// Extract the URI parameters required to setup the GCS KMS session.
	kmsURIParams, err := resolveKMSURIParams(kmsConsumeURL)
	if err != nil {
		return nil, err
	}

	// Client options to authenticate and start a GCS KMS session.
	// Currently, only accepting json of service account.
	var credentialsOpt []option.ClientOption

	switch kmsURIParams.auth {
	case "", cloud.AuthParamSpecified:
		if kmsURIParams.credentials != "" {
			authOption, err := createAuthOptionFromServiceAccountKey(kmsURIParams.credentials)
			if err != nil {
				return nil, errors.Wrapf(err, "error getting credentials from %s", CredentialsParam)
			}
			credentialsOpt = append(credentialsOpt, authOption)
		} else if kmsURIParams.bearerToken != "" {
			credentialsOpt = append(credentialsOpt, createAuthOptionFromBearerToken(kmsURIParams.bearerToken))
		} else {
			return nil, errors.Errorf(
				"%s or %s must be set if %q is %q",
				CredentialsParam,
				BearerTokenParam,
				cloud.AuthParam,
				cloud.AuthParamSpecified,
			)
		}
	case cloud.AuthParamImplicit:
		if env.KMSConfig().DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for gcs due to --external-io-disable-implicit-credentials flag")
		}
		// If implicit credentials used, no client options needed.
	default:
		return nil, errors.Errorf("unsupported value %s for %s", kmsURIParams.auth, cloud.AuthParam)
	}

	opts := []option.ClientOption{option.WithScopes(kms.DefaultAuthScopes()...)}
	if kmsURIParams.assumeRole == "" {
		opts = append(opts, credentialsOpt...)
	} else {
		assumeOpt, err := createImpersonateCredentials(ctx, kmsURIParams.assumeRole, kmsURIParams.delegateRoles, kms.DefaultAuthScopes(), credentialsOpt...)
		if err != nil {
			return nil, cloud.KMSInaccessible(errors.Wrapf(err, "failed to assume role"))
		}
		opts = append(opts, assumeOpt)
	}

	kmc, err := kms.NewKeyManagementClient(ctx, opts...)
	if err != nil {
		return nil, cloud.KMSInaccessible(err)
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
func (k *gcsKMS) MasterKeyID() string {
	return k.customerMasterKeyID
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
		return nil, cloud.KMSInaccessible(err)
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
		return nil, cloud.KMSInaccessible(err)
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
