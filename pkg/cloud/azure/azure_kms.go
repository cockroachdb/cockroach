// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	kms "github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/cloud"
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
}

// resolveKMSURIParams parses the `kmsURI` for all the supported KMS parameters.
func resolveKMSURIParams(kmsURI cloud.ConsumeURL) (kmsURIParams, error) {
	params := kmsURIParams{
		vaultName:    kmsURI.ConsumeParam(AzureVaultName),
		environment:  kmsURI.ConsumeParam(AzureEnvironmentKeyParam),
		clientID:     kmsURI.ConsumeParam(AzureClientIDParam),
		clientSecret: kmsURI.ConsumeParam(AzureClientSecretParam),
		tenantID:     kmsURI.ConsumeParam(AzureTenantIDParam),
	}

	// Validate that all the passed in parameters are supported.
	if unknownParams := kmsURI.RemainingQueryParams(); len(unknownParams) > 0 {
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

	kmsConsumeURL := cloud.ConsumeURL{URL: kmsURI}
	// Extract the URI parameters required to setup the Azure KMS session.
	kmsURIParams, err := resolveKMSURIParams(kmsConsumeURL)
	if err != nil {
		return nil, err
	}

	missingParams := make([]string, 0)
	if kmsURIParams.vaultName == "" {
		missingParams = append(missingParams, AzureVaultName)
	}
	if kmsURIParams.clientID == "" {
		missingParams = append(missingParams, AzureClientIDParam)
	}
	if kmsURIParams.clientSecret == "" {
		missingParams = append(missingParams, AzureClientSecretParam)
	}
	if kmsURIParams.tenantID == "" {
		missingParams = append(missingParams, AzureTenantIDParam)
	}
	if len(missingParams) != 0 {
		return nil, errors.Errorf("kms URI expected but did not receive: %s", strings.Join(missingParams, ", "))
	}

	if kmsURIParams.environment == "" {
		// Default to AzurePublicCloud if not specified for consistency with Azure Storage,
		// which itself defaults to this for backwards compatibility.
		kmsURIParams.environment = azure.PublicCloud.Name
	}

	//TODO(benbardin): Implicit auth.
	credential, err := azidentity.NewClientSecretCredential(kmsURIParams.tenantID, kmsURIParams.clientID, kmsURIParams.clientSecret, nil)
	if err != nil {
		return nil, errors.Wrap(err, "azure kms client secret credential")
	}

	azureEnv, err := azure.EnvironmentFromName(kmsURIParams.environment)
	if err != nil {
		return nil, errors.Wrap(err, "azure kms environment")
	}

	u, err := url.Parse(fmt.Sprintf("https://%s.%s", kmsURIParams.vaultName, azureEnv.KeyVaultDNSSuffix))
	if err != nil {
		return nil, errors.Wrap(err, "azure kms vault url")
	}
	client, err := kms.NewClient(u.String(), credential, nil)
	if err != nil {
		return nil, errors.Wrap(err, "azure kms vault client")
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

func (k *azureKMS) MasterKeyID() (string, error) {
	return k.customerMasterKeyID, nil
}

func (k *azureKMS) Encrypt(ctx context.Context, data []byte) ([]byte, error) {
	val, err := k.kms.Encrypt(ctx, k.customerMasterKeyID, k.customerMasterKeyVersion, kms.KeyOperationsParameters{
		Value:     data,
		Algorithm: &encryptionAlgorithm,
	}, nil)
	if err != nil {
		return nil, err
	}
	return val.Result, nil
}

func (k *azureKMS) Decrypt(ctx context.Context, data []byte) ([]byte, error) {
	val, err := k.kms.Decrypt(ctx, k.customerMasterKeyID, k.customerMasterKeyVersion, kms.KeyOperationsParameters{
		Value:     data,
		Algorithm: &encryptionAlgorithm,
	}, nil)
	if err != nil {
		return nil, err
	}
	return val.Result, nil
}

func (k *azureKMS) Close() error {
	// Azure KMS client does not implement Close.
	return nil
}
