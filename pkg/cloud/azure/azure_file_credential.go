package azure

import (
	"context"
	"encoding/json"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
)

var ApplicationCredentialsFile = envutil.EnvOrDefaultString("COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", "")

type FileCredential struct {
	fileName    string
	options     *FileCredentialOptions
	credentials azcore.TokenCredential
}

// FileCredentialOptions contains optional parameters for FileCredential
type FileCredentialOptions struct {
	azcore.ClientOptions
}

type azureCredentialsJSON struct {
	TenantID     string `json:"tenant_id"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
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

func NewDefaultAzureCredentialWithFile(
	options *azidentity.DefaultAzureCredentialOptions,
) (azcore.TokenCredential, error) {
	if options == nil {
		options = &azidentity.DefaultAzureCredentialOptions{}
	}

	// The Default credential supports env vars and managed identity magic.
	// We rely on the former for testing and the latter in prod.
	// https://learn.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential
	defaultCredentials, err := azidentity.NewDefaultAzureCredential(options)
	if err != nil {
		return nil, errors.Wrap(err, "default credential")
	}

	fileCredentials, err := NewAzureFileCredential(ApplicationCredentialsFile, &FileCredentialOptions{ClientOptions: options.ClientOptions})
	if err != nil {
		return nil, errors.Wrap(err, "file credential")
	}

	credential, err := azidentity.NewChainedTokenCredential([]azcore.TokenCredential{fileCredentials, defaultCredentials}, nil)
	if err != nil {
		return nil, errors.Wrap(err, "chaining credentials")
	}

	return credential, nil
}

// TODO: as part of a chain this may get retried even if file doesn't exist,
// maybe it needs to implement non-retriable on errors?
func (c *FileCredential) GetToken(
	ctx context.Context, options policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	token, err := c.credentials.GetToken(ctx, options)
	if err != nil {
		// TODO: parse the error explicitly instead of reloading the file every time.
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
	var credsJSON azureCredentialsJSON
	if err := json.Unmarshal(data, &credsJSON); err != nil {
		return errors.Wrapf(err, "failed to unmarshal credentials")
	}

	credentials, err := getCredentialsFromJSON(credsJSON, c.options.ClientOptions)
	if err != nil {
		return errors.Wrapf(err, "invalid credentials JSON")
	}

	c.credentials = credentials
	return nil
}

func getCredentialsFromJSON(
	json azureCredentialsJSON, clientOptions azcore.ClientOptions,
) (azcore.TokenCredential, error) {
	if json.TenantID == "" {
		return nil, errors.New("missing tenant ID")
	}

	if json.ClientID == "" {
		return nil, errors.New("missing client ID")
	}

	if json.ClientSecret == "" {
		return nil, errors.New("missing client secret")
	}

	o := &azidentity.ClientSecretCredentialOptions{ClientOptions: clientOptions}
	return azidentity.NewClientSecretCredential(json.TenantID, json.ClientID, json.ClientSecret, o)
}
