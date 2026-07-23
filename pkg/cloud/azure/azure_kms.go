// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	kms "github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/errors"
)

const (
	kmsScheme = "azure-kms"

	AzureVaultName = "AZURE_VAULT_NAME"
)

type azureKMS struct {
	kms                      *kms.Client
	customerMasterKeyID      string
	customerMasterKeyVersion string
}

var _ cloud.KMS = &azureKMS{}

// At time of writing, Azure KeyVault supports three encryption algorithms:
// https://learn.microsoft.com/en-us/azure/key-vault/keys/about-keys-details
// All are fine choices, but this is the most modern algorithm.
var encryptionAlgorithm = kms.JSONWebKeyEncryptionAlgorithmRSAOAEP256

func init() {
	cloud.RegisterKMSFromURIFactory(MakeAzureKMS, kmsScheme)
}

type kmsURIParams struct {
	vaultName string

	// Documented in azure_storage.go
	environment  string
	clientID     string
	clientSecret string
	tenantID     string

	auth cloudpb.AzureAuth
}

// resolveKMSURIParams parses the `kmsURI` for all the supported KMS parameters.
func resolveKMSURIParams(kmsURI *url.URL) (kmsURIParams, error) {
	kmsConsumeURL := cloud.ConsumeURL{URL: kmsURI}
	auth, err := azureAuthMethod(kmsURI, &kmsConsumeURL)
	if err != nil {
		return kmsURIParams{}, err
	}
	if !(auth == cloudpb.AzureAuth_EXPLICIT || auth == cloudpb.AzureAuth_IMPLICIT) {
		return kmsURIParams{}, errors.New("azure kms requires explicit auth (RBAC) or implicit auth")
	}
	params := kmsURIParams{
		vaultName:    kmsConsumeURL.ConsumeParam(AzureVaultName),
		environment:  kmsConsumeURL.ConsumeParam(AzureEnvironmentKeyParam),
		clientID:     kmsConsumeURL.ConsumeParam(AzureClientIDParam),
		clientSecret: kmsConsumeURL.ConsumeParam(AzureClientSecretParam),
		tenantID:     kmsConsumeURL.ConsumeParam(AzureTenantIDParam),
		auth:         auth,
	}

	// Validate that all the passed in parameters are supported.
	if unknownParams := kmsConsumeURL.RemainingQueryParams(); len(unknownParams) > 0 {
		return kmsURIParams{}, errors.Errorf(
			`unknown KMS query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return params, nil
}

func MakeAzureKMS(ctx context.Context, uri string, env cloud.KMSEnv) (cloud.KMS, error) {
	if env.KMSConfig().DisableOutbound {
		return nil, errors.New("external IO must be enabled to use KMS")
	}
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}
	if kmsURI.Path == "/" {
		return nil, errors.Newf("path component of the KMS cannot be empty; must contain the Customer Managed Key")
	}
	// Extract the URI parameters required to setup the Azure KMS session.
	kmsURIParams, err := resolveKMSURIParams(kmsURI)
	if err != nil {
		return nil, err
	}

	missingParams := make([]string, 0)
	extraParams := make([]string, 0)
	if kmsURIParams.vaultName == "" {
		missingParams = append(missingParams, AzureVaultName)
	}

	if kmsURIParams.auth == cloudpb.AzureAuth_EXPLICIT {
		if kmsURIParams.clientID == "" {
			missingParams = append(missingParams, AzureClientIDParam)
		}
		if kmsURIParams.clientSecret == "" {
			missingParams = append(missingParams, AzureClientSecretParam)
		}
		if kmsURIParams.tenantID == "" {
			missingParams = append(missingParams, AzureTenantIDParam)
		}
	}

	if kmsURIParams.auth == cloudpb.AzureAuth_IMPLICIT {
		if kmsURIParams.clientID != "" {
			extraParams = append(extraParams, AzureClientIDParam)
		}
		if kmsURIParams.clientSecret != "" {
			extraParams = append(extraParams, AzureClientSecretParam)
		}
		if kmsURIParams.tenantID != "" {
			extraParams = append(extraParams, AzureTenantIDParam)
		}
	}

	if len(missingParams) != 0 {
		return nil, errors.Errorf("kms URI expected but did not receive: %s", strings.Join(missingParams, ", "))
	}
	if len(extraParams) != 0 {
		return nil, errors.Errorf("kms URI does not support: %s", strings.Join(missingParams, ", "))
	}

	if kmsURIParams.environment == "" {
		// Default to AzurePublicCloud if not specified for consistency with Azure Storage,
		// which itself defaults to this for backwards compatibility.
		kmsURIParams.environment = azure.PublicCloud.Name
	}
	azureEnv, err := azure.EnvironmentFromName(kmsURIParams.environment)
	if err != nil {
		return nil, errors.Wrap(err, "azure kms environment")
	}

	u, err := url.Parse(fmt.Sprintf("https://%s.%s", kmsURIParams.vaultName, azureEnv.KeyVaultDNSSuffix))
	if err != nil {
		return nil, errors.Wrap(err, "azure kms vault url")
	}

	var credential azcore.TokenCredential

	switch kmsURIParams.auth {
	case cloudpb.AzureAuth_EXPLICIT:
		credential, err = azidentity.NewClientSecretCredential(kmsURIParams.tenantID, kmsURIParams.clientID, kmsURIParams.clientSecret, nil)
		if err != nil {
			return nil, errors.Wrap(err, "azure kms client secret credential")
		}
	case cloudpb.AzureAuth_IMPLICIT:
		if env.KMSConfig().DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for azure due to --external-io-disable-implicit-credentials flag")
		}

		credential, err = NewDefaultAzureCredentialWithFile(nil)
		if err != nil {
			return nil, errors.Wrap(err, "azure default credential")
		}
	default:
		return nil, errors.Errorf("azure kms unsupported auth value: %v", kmsURIParams.auth)
	}

	client, err := kms.NewClient(u.String(), credential, nil)
	if err != nil {
		return nil, cloud.KMSInaccessible(errors.Wrap(err, "azure kms vault client"))
	}

	keyTokens := strings.Split(strings.TrimPrefix(kmsURI.Path, "/"), "/")
	if len(keyTokens) != 2 {
		return nil, errors.New("azure kms key must be of form 'id/version'")
	}

	return &azureKMS{
		kms:                      client,
		customerMasterKeyID:      keyTokens[0],
		customerMasterKeyVersion: keyTokens[1],
	}, nil
}

func (k *azureKMS) MasterKeyID() string {
	return k.customerMasterKeyID
}

func (k *azureKMS) Encrypt(ctx context.Context, data []byte) ([]byte, error) {
	val, err := k.kms.Encrypt(ctx, k.customerMasterKeyID, k.customerMasterKeyVersion, kms.KeyOperationsParameters{
		Value:     data,
		Algorithm: &encryptionAlgorithm,
	}, nil)
	if err != nil {
		return nil, cloud.KMSInaccessible(err)
	}
	return val.Result, nil
}

func (k *azureKMS) Decrypt(ctx context.Context, data []byte) ([]byte, error) {
	val, err := k.kms.Decrypt(ctx, k.customerMasterKeyID, k.customerMasterKeyVersion, kms.KeyOperationsParameters{
		Value:     data,
		Algorithm: &encryptionAlgorithm,
	}, nil)
	if err != nil {
		return nil, cloud.KMSInaccessible(err)
	}
	return val.Result, nil
}

func (k *azureKMS) Close() error {
	// Azure KMS client does not implement Close.
	return nil
}
