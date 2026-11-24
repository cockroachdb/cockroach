// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	satypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/service-accounts/types"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// buildAuthCmd creates the parent "auth" command that groups all auth
// subcommands.
func (cr *commandRegistry) buildAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "authentication and authorization commands",
		Long: `Commands for managing authentication, service accounts, tokens,
and permissions.`,
	}
	cmd.AddCommand(
		cr.buildLoginCmd(),
		cr.buildLogoutCmd(),
		cr.buildWhoamiCmd(),
		cr.buildServiceAccountsCmd(),
	)
	return cmd
}

// buildServiceAccountsCmd creates the parent command for service account
// management.
func (cr *commandRegistry) buildServiceAccountsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "service-accounts",
		Aliases: []string{"sa"},
		Short:   "manage service accounts",
	}
	cmd.AddCommand(
		cr.buildSACreateCmd(),
		cr.buildSAListCmd(),
		cr.buildSADeleteCmd(),
		cr.buildSATokensCmd(),
		cr.buildSAOriginsCmd(),
		cr.buildSAPermissionsCmd(),
	)
	return cmd
}

// buildSACreateCmd creates a new service account.
func (cr *commandRegistry) buildSACreateCmd() *cobra.Command {
	var (
		nameFlag        string
		descriptionFlag string
		orphanFlag      bool
	)

	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a service account",
		Args:  cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			if nameFlag == "" {
				return errors.New("--name is required")
			}

			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			sa, err := c.CreateServiceAccount(
				context.Background(), l, satypes.CreateServiceAccountRequest{
					Name:        nameFlag,
					Description: descriptionFlag,
					Orphan:      orphanFlag,
				},
			)
			if err != nil {
				return errors.Wrap(err, "create service account")
			}

			fmt.Printf("ID:          %s\n", sa.ID)
			fmt.Printf("Name:        %s\n", sa.Name)
			fmt.Printf("Enabled:     %v\n", sa.Enabled)
			fmt.Printf("Description: %s\n", sa.Description)
			fmt.Printf("Created:     %s\n", sa.CreatedAt)
			return nil
		}),
	}

	cmd.Flags().StringVar(&nameFlag, "name", "", "service account name (required)")
	cmd.Flags().StringVar(
		&descriptionFlag, "description", "", "service account description",
	)
	cmd.Flags().BoolVar(&orphanFlag, "orphan", true, "whether the service account inherits permissions from its creator")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSAListCmd lists all service accounts.
func (cr *commandRegistry) buildSAListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list service accounts",
		Args:  cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			accounts, err := c.ListServiceAccounts(context.Background(), l)
			if err != nil {
				return errors.Wrap(err, "list service accounts")
			}

			if len(accounts) == 0 {
				fmt.Println("No service accounts found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "ID\tNAME\tENABLED\tCREATED\n")
			for _, sa := range accounts {
				fmt.Fprintf(tw, "%s\t%s\t%v\t%s\n",
					sa.ID, sa.Name, sa.Enabled, sa.CreatedAt,
				)
			}
			return tw.Flush()
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSADeleteCmd deletes a service account by ID.
func (cr *commandRegistry) buildSADeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "delete a service account",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.DeleteServiceAccount(
				context.Background(), l, args[0],
			); err != nil {
				return errors.Wrap(err, "delete service account")
			}

			fmt.Printf("Service account %s deleted.\n", args[0])
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSATokensCmd creates the parent command for service account token
// management.
func (cr *commandRegistry) buildSATokensCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tokens",
		Short: "manage service account tokens",
	}
	cmd.AddCommand(
		cr.buildSATokenListCmd(),
		cr.buildSATokenMintCmd(),
		cr.buildSATokenRevokeCmd(),
	)
	return cmd
}

// buildSATokenListCmd lists tokens for a service account.
func (cr *commandRegistry) buildSATokenListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <sa-id>",
		Short: "list tokens for a service account",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			tokens, err := c.ListServiceAccountTokens(
				context.Background(), l, args[0],
			)
			if err != nil {
				return errors.Wrap(err, "list tokens")
			}

			if len(tokens) == 0 {
				fmt.Println("No tokens found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "ID\tSUFFIX\tTYPE\tSTATUS\tEXPIRES\n")
			for _, t := range tokens {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					t.ID, t.TokenSuffix, t.TokenType, t.Status, t.ExpiresAt,
				)
			}
			return tw.Flush()
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSATokenMintCmd mints a new token for a service account.
func (cr *commandRegistry) buildSATokenMintCmd() *cobra.Command {
	var ttlDays int

	cmd := &cobra.Command{
		Use:   "mint <sa-id>",
		Short: "mint a new token for a service account",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			resp, err := c.MintServiceAccountToken(
				context.Background(), l, args[0], ttlDays,
			)
			if err != nil {
				return errors.Wrap(err, "mint token")
			}

			fmt.Printf("Token ID:  %s\n", resp.TokenID)
			fmt.Printf("Expires:   %s\n", resp.ExpiresAt)
			fmt.Printf("Token:     %s\n", resp.Token)
			fmt.Println()
			fmt.Println("Save this token now -- it cannot be retrieved later.")
			return nil
		}),
	}

	cmd.Flags().IntVar(&ttlDays, "ttl-days", 90, "token time-to-live in days")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSATokenRevokeCmd revokes a token for a service account.
func (cr *commandRegistry) buildSATokenRevokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke <sa-id> <token-id>",
		Short: "revoke a service account token",
		Args:  cobra.ExactArgs(2),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.RevokeServiceAccountToken(
				context.Background(), l, args[0], args[1],
			); err != nil {
				return errors.Wrap(err, "revoke token")
			}

			fmt.Printf("Token %s revoked.\n", args[1])
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSAOriginsCmd creates the parent command for service account origin
// management.
func (cr *commandRegistry) buildSAOriginsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "origins",
		Short: "manage service account allowed origins",
	}
	cmd.AddCommand(
		cr.buildSAOriginListCmd(),
		cr.buildSAOriginCreateCmd(),
		cr.buildSAOriginDeleteCmd(),
	)
	return cmd
}

// buildSAOriginListCmd lists allowed origins for a service account.
func (cr *commandRegistry) buildSAOriginListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <sa-id>",
		Short: "list allowed origins for a service account",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			origins, err := c.ListServiceAccountOrigins(
				context.Background(), l, args[0],
			)
			if err != nil {
				return errors.Wrap(err, "list origins")
			}

			if len(origins) == 0 {
				fmt.Println("No origins found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "ID\tCIDR\tDESCRIPTION\tCREATED\n")
			for _, o := range origins {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
					o.ID, o.CIDR, o.Description, o.CreatedAt,
				)
			}
			return tw.Flush()
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSAOriginCreateCmd creates an allowed origin for a service account.
func (cr *commandRegistry) buildSAOriginCreateCmd() *cobra.Command {
	var (
		cidrFlag        string
		descriptionFlag string
	)

	cmd := &cobra.Command{
		Use:   "create <sa-id>",
		Short: "create an allowed origin for a service account",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			if cidrFlag == "" {
				return errors.New("--cidr is required")
			}

			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			origin, err := c.CreateServiceAccountOrigin(
				context.Background(), l, args[0], cidrFlag, descriptionFlag,
			)
			if err != nil {
				return errors.Wrap(err, "create origin")
			}

			fmt.Printf("ID:          %s\n", origin.ID)
			fmt.Printf("CIDR:        %s\n", origin.CIDR)
			fmt.Printf("Description: %s\n", origin.Description)
			fmt.Printf("Created:     %s\n", origin.CreatedAt)
			return nil
		}),
	}

	cmd.Flags().StringVar(&cidrFlag, "cidr", "", "CIDR block (required)")
	cmd.Flags().StringVar(
		&descriptionFlag, "description", "", "origin description",
	)
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSAOriginDeleteCmd deletes an allowed origin for a service account.
func (cr *commandRegistry) buildSAOriginDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <sa-id> <origin-id>",
		Short: "delete an allowed origin for a service account",
		Args:  cobra.ExactArgs(2),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.DeleteServiceAccountOrigin(
				context.Background(), l, args[0], args[1],
			); err != nil {
				return errors.Wrap(err, "delete origin")
			}

			fmt.Printf("Origin %s deleted.\n", args[1])
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSAPermissionsCmd creates the parent command for service account
// permission management.
func (cr *commandRegistry) buildSAPermissionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "permissions",
		Short: "manage service account permissions",
	}
	cmd.AddCommand(
		cr.buildSAPermissionListCmd(),
		cr.buildSAPermissionUpdateCmd(),
	)
	return cmd
}

// buildSAPermissionListCmd lists permissions for a service account.
func (cr *commandRegistry) buildSAPermissionListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <sa-id>",
		Short: "list permissions for a service account",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			perms, err := c.ListServiceAccountPermissions(
				context.Background(), l, args[0],
			)
			if err != nil {
				return errors.Wrap(err, "list permissions")
			}

			if len(perms) == 0 {
				fmt.Println("No permissions found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "ID\tSCOPE\tPERMISSION\tCREATED\n")
			for _, p := range perms {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
					p.ID, p.Scope, p.Permission, p.CreatedAt,
				)
			}
			return tw.Flush()
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildSAPermissionUpdateCmd replaces all permissions for a service account.
func (cr *commandRegistry) buildSAPermissionUpdateCmd() *cobra.Command {
	var permissionsJSON string

	cmd := &cobra.Command{
		Use:   "update <sa-id>",
		Short: "update permissions for a service account",
		Long: `Replace all permissions for a service account.

The --permissions flag accepts a JSON array of permission entries.

Example:
  roachprod auth service-accounts permissions update <sa-id> \
    --permissions '[{"scope":"env:test","permission":"clusters:create"}]'
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			if permissionsJSON == "" {
				return errors.New("--permissions is required")
			}

			var permissions []satypes.AddPermissionRequest
			if err := json.Unmarshal(
				[]byte(permissionsJSON), &permissions,
			); err != nil {
				return errors.Wrap(err, "parse --permissions JSON")
			}

			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.UpdateServiceAccountPermissions(
				context.Background(), l, args[0], permissions,
			); err != nil {
				return errors.Wrap(err, "update permissions")
			}

			fmt.Printf(
				"Permissions updated for service account %s.\n", args[0],
			)
			return nil
		}),
	}

	cmd.Flags().StringVar(
		&permissionsJSON, "permissions", "",
		`JSON array of permissions (required), e.g. '[{"scope":"env:test","permission":"clusters:create"}]'`,
	)
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}
