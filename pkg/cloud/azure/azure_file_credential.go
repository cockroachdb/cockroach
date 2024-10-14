// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"context"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

type FileCredential struct {
	fileName    string
	options     *FileCredentialOptions
	credentials azcore.TokenCredential
}

// FileCredentialOptions contains optional parameters for FileCredential
type FileCredentialOptions struct {
	azcore.ClientOptions
	testingKnobs *TestingKnobs
}

type DefaultAzureCredentialWithFileOptions struct {
	azcore.ClientOptions
	testingKnobs *TestingKnobs
}

type azureCredentialsYAML struct {
	AzureTenantID     string `yaml:"azure_tenant_id"`
	AzureClientID     string `yaml:"azure_client_id"`
	AzureClientSecret string `yaml:"azure_client_secret"`
}

var _ azcore.TokenCredential = &FileCredential{}

func NewAzureFileCredential(
	fileName string, options *FileCredentialOptions,
) (*FileCredential, error) {
	if fileName == "" {
		return nil, errors.Newf("credentials file path provided is empty")
	}

	if options == nil {
		options = &FileCredentialOptions{}
	}

	c := &FileCredential{fileName: fileName, options: options}
	if err := c.reloadCredentialsFromFile(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewDefaultAzureCredentialWithFile creates a credential chain that's simply
// a FileCredential prepended to the chain in DefaultAzureCredential.
func NewDefaultAzureCredentialWithFile(
	options *DefaultAzureCredentialWithFileOptions,
) (azcore.TokenCredential, error) {
	if options == nil {
		options = &DefaultAzureCredentialWithFileOptions{}
	}

	var credentials []azcore.TokenCredential
	var credErrors []string

	credentialFileName := envutil.EnvOrDefaultString("COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", "")
	fileCredentials, err := NewAzureFileCredential(credentialFileName, &FileCredentialOptions{ClientOptions: options.ClientOptions, testingKnobs: options.testingKnobs})
	if err != nil {
		credErrors = append(credErrors, errors.Wrap(err, "file credential").Error())
	} else {
		credentials = append(credentials, fileCredentials)
	}

	// The Default credential supports env vars and managed identity magic.
	// We rely on the former for testing and the latter in prod.
	// https://learn.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential
	defaultCredentials, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{ClientOptions: options.ClientOptions})
	if err != nil {
		credErrors = append(credErrors, errors.Wrap(err, "default credential").Error())
	} else {
		credentials = append(credentials, defaultCredentials)
	}

	if len(credentials) == 0 {
		errorMessage := strings.Join(credErrors, "\n\t")
		return nil, errors.Newf("default credential with file: %s", errorMessage)
	}

	credential, err := azidentity.NewChainedTokenCredential(credentials, nil)
	if err != nil {
		return nil, errors.Wrap(err, "chaining credentials")
	}

	return credential, nil
}

// GetToken attempts to get a token using the credentials created by the
// underlying file. If an error is encountered while getting a token, the method
// will retry once by reloading and recreating the credential again, in case
// the contents of the underlying file has changed.
func (c *FileCredential) GetToken(
	ctx context.Context, options policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	token, err := c.credentials.GetToken(ctx, options)
	if knobs := c.options.testingKnobs; knobs != nil && knobs.MapFileCredentialToken != nil {
		token, err = knobs.MapFileCredentialToken(token, err)
	}

	if err != nil {
		log.Infof(ctx, "reloading file credentials from %s", c.fileName)
		if err := c.reloadCredentialsFromFile(); err != nil {
			return azcore.AccessToken{}, err
		}

		return c.credentials.GetToken(ctx, options)
	}

	return token, nil
}

func (c *FileCredential) reloadCredentialsFromFile() error {
	// Read the credentials file.
	data, err := os.ReadFile(c.fileName)
	if err != nil {
		return err
	}

	// Unmarshal the credentials.
	var credsYAML azureCredentialsYAML
	if err := yaml.UnmarshalStrict(data, &credsYAML); err != nil {
		return errors.Wrapf(err, "failed to unmarshal credentials")
	}

	credentials, err := getCredentialsFromYAML(credsYAML, c.options.ClientOptions)
	if err != nil {
		return errors.Wrapf(err, "invalid credentials JSON")
	}

	c.credentials = credentials
	return nil
}

func getCredentialsFromYAML(
	yaml azureCredentialsYAML, clientOptions azcore.ClientOptions,
) (azcore.TokenCredential, error) {
	if yaml.AzureTenantID == "" {
		return nil, errors.New("missing tenant ID")
	}

	if yaml.AzureClientID == "" {
		return nil, errors.New("missing client ID")
	}

	if yaml.AzureClientSecret == "" {
		return nil, errors.New("missing client secret")
	}

	o := &azidentity.ClientSecretCredentialOptions{ClientOptions: clientOptions}
	return azidentity.NewClientSecretCredential(yaml.AzureTenantID, yaml.AzureClientID, yaml.AzureClientSecret, o)
}
