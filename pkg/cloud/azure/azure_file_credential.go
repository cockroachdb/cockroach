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
	fileName string
	options  *FileCredentialOptions
}

// FileCredentialOptions contains optional parameters for FileCredential
type FileCredentialOptions struct {
	azcore.ClientOptions
}

type azureCredentialsJSON struct {
	tenantID     string
	clientID     string
	clientSecret string
	username     string
	password     string
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

	return &FileCredential{fileName: fileName, options: options}, nil
}

func NewAzureFileEnvVarCredential() (*FileCredential, error) {
	return NewAzureFileCredential(ApplicationCredentialsFile, nil)
}

// TODO: as part of a chain this may get retried even if file doesn't exist,
// maybe it needs to implement non-retriable on errors?
func (c *FileCredential) GetToken(
	ctx context.Context, options policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	// Read the credentials file.
	data, err := os.ReadFile(c.fileName)
	if err != nil {
		return azcore.AccessToken{}, err
	}

	// Unmarshal the credentials.
	var credsJSON azureCredentialsJSON
	if err := json.Unmarshal(data, &credsJSON); err != nil {
		return azcore.AccessToken{}, errors.Wrapf(err, "failed to unmarshal credentials")
	}

	credentials, err := getCredentialsFromJSON(credsJSON, c.options.ClientOptions)
	if err != nil {
		return azcore.AccessToken{}, errors.Wrapf(err, "invalid credentials JSON")
	}

	return credentials.GetToken(ctx, options)
}

func getCredentialsFromJSON(
	json azureCredentialsJSON, clientOptions azcore.ClientOptions,
) (azcore.TokenCredential, error) {
	if json.tenantID == "" {
		return nil, errors.New("missing tenant ID")
	}

	if json.clientID == "" {
		return nil, errors.New("missing client ID")
	}

	if json.clientSecret != "" {
		o := &azidentity.ClientSecretCredentialOptions{ClientOptions: clientOptions}
		return azidentity.NewClientSecretCredential(json.tenantID, json.clientID, json.clientSecret, o)
	}

	if json.username != "" {
		if json.password != "" {
			o := &azidentity.UsernamePasswordCredentialOptions{ClientOptions: clientOptions}
			return azidentity.NewUsernamePasswordCredential(json.tenantID, json.clientID, json.username, json.password, o)
		}
		return nil, errors.New("no value for password")
	}
	return nil, errors.New("incomplete credentials JSON with only tenant ID and client ID")
}
