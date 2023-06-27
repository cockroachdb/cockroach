package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestAzureFileCredential(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		fmt.Println("@@@ err", err)
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}

	ctx := context.Background()
	const filePath = "credentials.json"
	tmpDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	p := path.Join(tmpDir, filePath)

	azJSON := azureCredentialsJSON{
		TenantID:     cfg.tenantID,
		ClientID:     cfg.clientID,
		ClientSecret: cfg.clientSecret,
	}

	bytes, err := json.Marshal(azJSON)
	require.NoError(t, err)

	fmt.Println("@@@ js", string(bytes), "STRUCT", azJSON,
		os.Getenv("AZURE_CLIENT_ID"),
		redact.Safe(os.Getenv("AZURE_CLIENT_SECRET")),
		redact.Safe(os.Getenv("AZURE_TENANT_ID")))
	require.NoError(t, os.WriteFile(p, bytes, 0600))

	cred, err := NewAzureFileCredential(p, nil)
	require.NoError(t, err)

	client, err := service.NewClient(fmt.Sprintf("https://%s.blob.%s", cfg.account, azure.PublicCloud.StorageEndpointSuffix), cred, nil)
	require.NoError(t, err)

	properties, err := client.GetProperties(ctx, nil)
	require.NoError(t, err)

	fmt.Println("@@@ done", properties)
}
