// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/environments/types"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// buildEnvironmentCmd creates the parent "environment" command.
func (cr *commandRegistry) buildEnvironmentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "environment",
		Aliases: []string{"env"},
		Short:   "manage provisioning environments",
		Long:    `Commands for managing provisioning environments and their variables.`,
	}
	cmd.AddCommand(
		cr.buildEnvListCmd(),
		cr.buildEnvCreateCmd(),
		cr.buildEnvDeleteCmd(),
		cr.buildEnvVariableCmd(),
	)
	return cmd
}

// buildEnvListCmd lists all environments.
func (cr *commandRegistry) buildEnvListCmd() *cobra.Command {
	var outputFlag string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "list environments",
		Args:  cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			envs, err := c.ListEnvironments(context.Background(), l)
			if err != nil {
				return errors.Wrap(err, "list environments")
			}

			if outputFlag == "json" {
				return printJSON(envs)
			}

			if len(envs) == 0 {
				fmt.Println("No environments found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "NAME\tDESCRIPTION\tOWNER\tCREATED\n")
			for _, e := range envs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
					e.Name, e.Description, e.Owner,
					e.CreatedAt.Format("2006-01-02 15:04"),
				)
			}
			return tw.Flush()
		}),
	}

	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildEnvCreateCmd creates a new environment.
func (cr *commandRegistry) buildEnvCreateCmd() *cobra.Command {
	var descriptionFlag string

	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "create an environment",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			env, err := c.CreateEnvironment(
				context.Background(), l, envtypes.InputCreateEnvironmentDTO{
					Name:        args[0],
					Description: descriptionFlag,
				},
			)
			if err != nil {
				return errors.Wrap(err, "create environment")
			}

			fmt.Printf("Environment %q created.\n", env.Name)
			return nil
		}),
	}

	cmd.Flags().StringVar(
		&descriptionFlag, "description", "", "environment description",
	)
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildEnvDeleteCmd deletes an environment.
func (cr *commandRegistry) buildEnvDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "delete an environment",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.DeleteEnvironment(
				context.Background(), l, args[0],
			); err != nil {
				return errors.Wrap(err, "delete environment")
			}

			fmt.Printf("Environment %q deleted.\n", args[0])
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildEnvVariableCmd creates the parent "variable" command.
func (cr *commandRegistry) buildEnvVariableCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "variable",
		Aliases: []string{"var"},
		Short:   "manage environment variables",
	}
	cmd.AddCommand(
		cr.buildEnvVarListCmd(),
		cr.buildEnvVarGetCmd(),
		cr.buildEnvVarSetCmd(),
		cr.buildEnvVarDeleteCmd(),
	)
	return cmd
}

// buildEnvVarListCmd lists variables for an environment.
func (cr *commandRegistry) buildEnvVarListCmd() *cobra.Command {
	var outputFlag string

	cmd := &cobra.Command{
		Use:   "list <env>",
		Short: "list variables for an environment",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			vars, err := c.ListEnvironmentVariables(
				context.Background(), l, args[0],
			)
			if err != nil {
				return errors.Wrap(err, "list variables")
			}

			if outputFlag == "json" {
				return printJSON(vars)
			}

			if len(vars) == 0 {
				fmt.Println("No variables found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "KEY\tTYPE\tVALUE\tUPDATED\n")
			for _, v := range vars {
				value := v.Value
				if v.Type == envmodels.VarTypeSecret {
					value = "********"
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
					v.Key, v.Type, value,
					v.UpdatedAt.Format("2006-01-02 15:04"),
				)
			}
			return tw.Flush()
		}),
	}

	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildEnvVarGetCmd gets a specific variable.
func (cr *commandRegistry) buildEnvVarGetCmd() *cobra.Command {
	var outputFlag string

	cmd := &cobra.Command{
		Use:   "get <env> <key>",
		Short: "get a variable value",
		Args:  cobra.ExactArgs(2),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			v, err := c.GetEnvironmentVariable(
				context.Background(), l, args[0], args[1],
			)
			if err != nil {
				return errors.Wrap(err, "get variable")
			}

			if outputFlag == "json" {
				return printJSON(v)
			}

			fmt.Printf("Key:     %s\n", v.Key)
			fmt.Printf("Type:    %s\n", v.Type)
			if v.Type == envmodels.VarTypeSecret {
				fmt.Printf("Value:   ********\n")
			} else {
				fmt.Printf("Value:   %s\n", v.Value)
			}
			fmt.Printf("Updated: %s\n", v.UpdatedAt.Format("2006-01-02 15:04:05"))
			return nil
		}),
	}

	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildEnvVarSetCmd sets (creates or updates) a variable.
func (cr *commandRegistry) buildEnvVarSetCmd() *cobra.Command {
	var secretFlag bool

	cmd := &cobra.Command{
		Use:   "set <env> <key> <value>",
		Short: "set a variable (creates or updates)",
		Args:  cobra.ExactArgs(3),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			envName, key, value := args[0], args[1], args[2]

			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			varType := envmodels.VarTypePlaintext
			if secretFlag {
				varType = envmodels.VarTypeSecret
			}

			ctx := context.Background()

			// Upsert: try create, on 409 conflict fall back to update.
			createDTO := envtypes.InputCreateVariableDTO{
				Key:   key,
				Value: value,
				Type:  varType,
			}
			_, createErr := c.CreateEnvironmentVariable(ctx, l, envName, createDTO)
			if createErr == nil {
				fmt.Printf("Variable %q created.\n", key)
				return nil
			}
			var httpErr *client.HTTPError
			if errors.As(createErr, &httpErr) && httpErr.StatusCode == http.StatusConflict {
				updateDTO := envtypes.InputUpdateVariableDTO{
					Value: value,
					Type:  varType,
				}
				_, updateErr := c.UpdateEnvironmentVariable(ctx, l, envName, key, updateDTO)
				if updateErr != nil {
					return errors.Wrap(updateErr, "update variable")
				}
				fmt.Printf("Variable %q updated.\n", key)
				return nil
			}
			return errors.Wrap(createErr, "set variable")
		}),
	}

	cmd.Flags().BoolVar(&secretFlag, "secret", false, "store as secret")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildEnvVarDeleteCmd deletes a variable.
func (cr *commandRegistry) buildEnvVarDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <env> <key>",
		Short: "delete a variable",
		Args:  cobra.ExactArgs(2),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.DeleteEnvironmentVariable(
				context.Background(), l, args[0], args[1],
			); err != nil {
				return errors.Wrap(err, "delete variable")
			}

			fmt.Printf("Variable %q deleted.\n", args[1])
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}
