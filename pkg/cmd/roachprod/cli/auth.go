// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client/auth"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

func (cr *commandRegistry) buildLoginCmd() *cobra.Command {
	var tokenFlag string

	cmd := &cobra.Command{
		Use:   "login",
		Short: "authenticate with roachprod-centralized",
		Long: `Authenticate with the roachprod-centralized API.

Without flags, initiates Okta Device Flow for interactive login.
With --token, stores the provided service account token directly.

Examples:
  # Interactive login via Okta
  roachprod login

  # Login with a service account token
  roachprod login --token=rp$sa$1$...
`,
		Args: cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			if tokenFlag != "" {
				return loginWithToken(tokenFlag)
			}
			return loginWithOkta(cmd.Context())
		}),
	}

	cmd.Flags().StringVar(&tokenFlag, "token", "", "service account token (for non-interactive login)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

func (cr *commandRegistry) buildLogoutCmd() *cobra.Command {
	var revokeFlag bool

	cmd := &cobra.Command{
		Use:   "logout",
		Short: "clear stored credentials",
		Long: `Clear stored credentials and optionally revoke the token on the server.

This command:
1. For user tokens: revokes the token on the server, then clears local credentials
2. For service account tokens: only clears local credentials (use --revoke to also revoke on server)
3. Clears the stored token from the OS keyring

Note: If using the ` + auth.EnvAPIToken + ` environment variable, you must unset it manually.
`,
		Args: cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			return logout(revokeFlag)
		}),
	}

	cmd.Flags().BoolVar(&revokeFlag, "revoke", false, "revoke service account tokens on the server (user tokens are always revoked)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

func (cr *commandRegistry) buildWhoamiCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "whoami",
		Short: "display current authenticated principal",
		Long: `Display information about the currently authenticated principal.

Shows:
- Principal email or service account name
- Token type (user or service_account)
- Token expiration time
`,
		Args: cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			return whoami()
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// loginWithToken validates a token against the server and stores it with
// its expiration date. If validation fails for any reason (bad token,
// network error, misconfigured client), the token is not stored.
func loginWithToken(token string) error {
	tokenSource, err := auth.NewBearerTokenSource()
	if err != nil {
		return errors.Wrap(err, "failed to initialize token storage")
	}

	// Store the token temporarily so the HTTP client can use it for validation.
	storedToken := auth.StoredToken{
		Token: token,
	}
	if err := tokenSource.StoreToken(storedToken); err != nil {
		return errors.Wrap(err, "failed to store token")
	}

	// Validate the token against the server.
	c, l, err := newAuthClient()
	if err != nil {
		_ = tokenSource.ClearToken()
		return errors.Wrap(err, "failed to create API client")
	}

	whoamiResp, err := c.WhoAmI(context.Background(), l)
	if err != nil {
		_ = tokenSource.ClearToken()
		if isAuthError(err) {
			return errors.Wrap(err, "token is invalid or expired")
		}
		return errors.Wrap(err, "failed to validate token with server")
	}

	// Token is valid. Update stored token with expiration from server.
	if whoamiResp.Token.ExpiresAt != "" {
		expiresAt, parseErr := time.Parse(
			"2006-01-02T15:04:05Z07:00", whoamiResp.Token.ExpiresAt,
		)
		if parseErr == nil {
			storedToken.ExpiresAt = expiresAt
			if storeErr := tokenSource.StoreToken(storedToken); storeErr != nil {
				fmt.Printf("Warning: Could not update token expiration: %v\n", storeErr)
			}
		}
	}

	fmt.Println("Token stored successfully.")
	if whoamiResp.User != nil {
		fmt.Printf("Authenticated as: %s (%s)\n",
			whoamiResp.User.Email, whoamiResp.User.Name)
	} else if whoamiResp.ServiceAccount != nil {
		fmt.Printf("Authenticated as service account: %s\n",
			whoamiResp.ServiceAccount.Name)
	}
	if whoamiResp.Token.ExpiresAt != "" {
		fmt.Printf("Token expires: %s\n", whoamiResp.Token.ExpiresAt)
	}

	return nil
}

// isAuthError returns true if the error indicates the token itself is
// invalid (as opposed to a network or server error).
func isAuthError(err error) bool {
	var httpErr *client.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusUnauthorized ||
			httpErr.StatusCode == http.StatusForbidden
	}
	return false
}

// loginWithOkta performs the Okta Device Authorization Grant flow.
func loginWithOkta(ctx context.Context) error {
	// Load Okta configuration
	oktaConfig := auth.LoadOktaConfigFromEnv()

	if oktaConfig.ClientID == "" {
		return errors.Newf("Okta client ID not configured; set %s environment variable", auth.EnvOktaClientID)
	}

	// Initialize the device flow
	flow := auth.NewOktaDeviceFlow(oktaConfig.Issuer, oktaConfig.ClientID)

	// Start device authorization
	fmt.Println("Starting authentication...")
	deviceCode, err := flow.StartDeviceAuthorization(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to start device authorization")
	}

	// Display instructions to the user
	fmt.Println()
	if deviceCode.VerificationURIComplete != "" {
		fmt.Printf("Opening browser to: %s\n", deviceCode.VerificationURIComplete)
		fmt.Println()
		// Try to open the browser
		if err := openBrowser(deviceCode.VerificationURIComplete); err != nil {
			fmt.Printf("If browser doesn't open, visit: %s\n", deviceCode.VerificationURI)
			fmt.Printf("Enter code: %s\n", deviceCode.UserCode)
		}
	} else {
		fmt.Printf("Visit: %s\n", deviceCode.VerificationURI)
		fmt.Printf("Enter code: %s\n", deviceCode.UserCode)
	}
	fmt.Println()
	fmt.Print("Waiting for authentication...")

	// Poll for token
	tokenResp, err := flow.PollForToken(ctx, deviceCode.DeviceCode, deviceCode.Interval)
	if err != nil {
		fmt.Println(" failed!")
		return errors.Wrap(err, "authentication failed")
	}
	fmt.Println(" done!")

	// Exchange the Okta ID token for a roachprod opaque token
	fmt.Println()
	fmt.Print("Exchanging token with roachprod-centralized...")

	exchangeResp, err := auth.ExchangeOktaToken(ctx, client.LoadConfigFromEnv().BaseURL, tokenResp.IDToken)
	if err != nil {
		fmt.Println(" failed!")
		return errors.Wrap(err, "token exchange failed")
	}
	fmt.Println(" done!")

	// Parse expiration time from response
	expiresAt, err := time.Parse("2006-01-02T15:04:05Z07:00", exchangeResp.ExpiresAt)
	if err != nil {
		// Fallback to a default if parsing fails
		expiresAt = timeutil.Now().Add(7 * 24 * time.Hour)
	}

	tokenSource, err := auth.NewBearerTokenSource()
	if err != nil {
		return errors.Wrap(err, "failed to initialize token storage")
	}

	storedToken := auth.StoredToken{
		Token:     exchangeResp.Token,
		ExpiresAt: expiresAt,
	}

	if err := tokenSource.StoreToken(storedToken); err != nil {
		return errors.Wrap(err, "failed to store token")
	}

	fmt.Println()
	fmt.Println("Successfully authenticated!")
	fmt.Printf("Token expires: %s\n", expiresAt.Format(time.RFC3339))

	return nil
}

// logout clears stored credentials and optionally revokes tokens on the server.
// For user tokens, revocation is always attempted.
// For service account tokens, revocation only happens if revokeServiceAccount is true.
func logout(revokeServiceAccount bool) error {
	ctx := context.Background()

	tokenSource, err := auth.NewBearerTokenSource()
	if err != nil {
		return errors.Wrap(err, "failed to initialize token storage")
	}

	// Check if there's a token to logout
	if !tokenSource.HasToken() {
		fmt.Println("No stored credentials found.")
		return nil
	}

	// Check if token is from env var (we can't clear those)
	envToken := auth.GetEnvToken()
	if envToken != "" {
		fmt.Printf("Warning: Token is set via %s environment variable.\n", auth.EnvAPIToken)
		fmt.Println("Cannot clear environment variable tokens. Unset the variable manually:")
		fmt.Printf("  unset %s\n", auth.EnvAPIToken)
		return nil
	}

	// Get the stored token
	storedToken, err := tokenSource.GetStoredToken()
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			fmt.Println("No stored credentials found.")
			return nil
		}
		return errors.Wrap(err, "failed to get token")
	}

	// Try to get token info from server to determine type and ID
	var tokenRevoked bool
	if !storedToken.IsExpired() {
		c, l, err := newAuthClient()
		if err != nil {
			fmt.Printf("Warning: Could not create API client: %v\n", err)
		} else {
			whoamiResp, err := c.WhoAmI(ctx, l)
			if err == nil && whoamiResp.Token.ID != "" {
				isServiceAccount := whoamiResp.ServiceAccount != nil
				shouldRevoke := !isServiceAccount || revokeServiceAccount

				if shouldRevoke {
					if err := c.RevokeToken(ctx, l, whoamiResp.Token.ID); err != nil {
						fmt.Printf("Warning: Could not revoke token on server: %v\n", err)
					} else {
						tokenRevoked = true
						if isServiceAccount {
							fmt.Println("Service account token revoked on server.")
						} else {
							fmt.Println("User token revoked on server.")
						}
					}
				} else {
					fmt.Println("Service account token not revoked (use --revoke to revoke on server).")
				}
			} else if err != nil {
				fmt.Printf("Warning: Could not verify token with server: %v\n", err)
			}
		}
	} else {
		fmt.Println("Token already expired, skipping server revocation.")
	}

	// Clear local credentials
	if err := tokenSource.ClearToken(); err != nil {
		return errors.Wrap(err, "failed to clear credentials")
	}

	if tokenRevoked {
		fmt.Println("Local credentials cleared.")
	} else {
		fmt.Println("Credentials cleared.")
	}

	return nil
}

// whoami displays information about the current authentication.
func whoami() error {
	tokenSource, err := auth.NewBearerTokenSource()
	if err != nil {
		return errors.Wrap(err, "failed to initialize token storage")
	}

	storedToken, err := tokenSource.GetStoredToken()
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			fmt.Println("Not authenticated. Run 'roachprod login' to authenticate.")
			return nil
		}
		return errors.Wrap(err, "failed to get token")
	}

	// Check if token is expired locally first
	if storedToken.IsExpired() {
		fmt.Println("Token has expired. Run 'roachprod login' to refresh.")
		return nil
	}

	// Call the server to get principal info
	c, l, err := newAuthClient()
	if err != nil {
		return errors.Wrap(err, "failed to create API client")
	}
	whoamiResp, err := c.WhoAmI(context.Background(), l)
	if err != nil {
		// If we can't reach the server, display local info
		fmt.Println("Warning: Could not reach server to verify authentication.")
		fmt.Printf("Error: %v\n", err)
		fmt.Println()
		fmt.Println("Local token info:")
		displayLocalTokenInfo(storedToken)
		return nil
	}

	// Display principal info from server
	if whoamiResp.User != nil {
		fmt.Printf("Principal: %s\n", whoamiResp.User.Email)
		fmt.Printf("Name: %s\n", whoamiResp.User.Name)
		fmt.Println("Type: user")
		fmt.Printf("Active: %v\n", whoamiResp.User.Active)
	} else if whoamiResp.ServiceAccount != nil {
		fmt.Printf("Principal: %s\n", whoamiResp.ServiceAccount.Name)
		if whoamiResp.ServiceAccount.Description != "" {
			fmt.Printf("Description: %s\n", whoamiResp.ServiceAccount.Description)
		}
		fmt.Println("Type: service_account")
		fmt.Printf("Enabled: %v\n", whoamiResp.ServiceAccount.Enabled)
	}

	// Token expiration from server
	if whoamiResp.Token.ExpiresAt != "" {
		expiresAt, err := time.Parse("2006-01-02T15:04:05Z07:00", whoamiResp.Token.ExpiresAt)
		if err == nil {
			if time.Until(expiresAt) < auth.ExpirationWarningThreshold {
				fmt.Printf("Token expires: %s (expires soon!)\n", expiresAt.Format(time.RFC3339))
			} else {
				fmt.Printf("Token expires: %s\n", expiresAt.Format(time.RFC3339))
			}
		}
	}

	// Display permissions if any
	if len(whoamiResp.Permissions) > 0 {
		fmt.Println()
		fmt.Println("Permissions:")
		for _, perm := range whoamiResp.Permissions {
			fmt.Printf("  - %s/%s: %s\n", perm.Provider, perm.Account, perm.Permission)
		}
	}

	return nil
}

// displayLocalTokenInfo shows token information from local storage.
func displayLocalTokenInfo(storedToken *auth.StoredToken) {
	fmt.Println("Authenticated: Yes (unverified)")

	// Check for expiration warning
	if storedToken.ExpiresWithin(auth.ExpirationWarningThreshold) {
		fmt.Printf("Token expires: %s (expires soon!)\n", storedToken.ExpiresAt.Format(time.RFC3339))
	} else if !storedToken.ExpiresAt.IsZero() {
		fmt.Printf("Token expires: %s\n", storedToken.ExpiresAt.Format(time.RFC3339))
	} else {
		fmt.Println("Token expires: No expiration set")
	}

	// Display masked token for debugging
	if len(storedToken.Token) > 20 {
		fmt.Printf("Token: %s...%s\n", storedToken.Token[:10], storedToken.Token[len(storedToken.Token)-6:])
	}
}

// newAuthClient creates a client.Client configured for auth CLI commands.
// It loads config from environment and forces the client to be enabled.
func newAuthClient() (*client.Client, *logger.Logger, error) {
	cfg := client.LoadConfigFromEnv()
	cfg.Enabled = true
	cfg.SilentFailures = true
	c, err := client.NewClient(client.WithConfig(cfg))
	if err != nil {
		return nil, nil, err
	}

	l, err := (&logger.Config{}).NewLogger("")
	if err != nil {
		return nil, nil, errors.Wrap(err, "create logger")
	}

	return c, l, nil
}

// openBrowser tries to open the default browser to the given URL.
func openBrowser(url string) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return errors.Newf("unsupported platform: %s", runtime.GOOS)
	}

	return cmd.Start()
}
