// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/spf13/cobra"
)

func PromptYesNo(msg string, defaultYes bool) bool {
	if defaultYes {
		fmt.Printf("%s y[default]/n: ", msg)
	} else {
		fmt.Printf("%s y/n[default]: ", msg)
	}

	var answer string
	_, _ = fmt.Scanln(&answer)
	answer = strings.TrimSpace(answer)

	isYes := answer == "y" || answer == "Y"
	isEmpty := answer == ""

	if defaultYes {
		return isYes || isEmpty
	}

	return isYes
}

// SwapBinary attempts to swap the `old` file with the `new` file. Used to
// update a running roachprod binary.
// Note: there is special handling if `new` points to a file ending in `.bak`.
// In this case, it is assumed to be a `revert` operation, in which case we
// do *not* backup the old/current file.
func SwapBinary(old, new string) error {
	destInfo, err := os.Stat(new)

	if err != nil {
		if oserror.IsNotExist(err) {
			return errors.WithDetail(err, "binary does not exist: "+new)
		}
		return err
	}

	if destInfo.IsDir() {
		return errors.Newf("binary path is a directory, not a file: %s", new)
	}

	oldInfo, err := os.Stat(old)
	if err != nil {
		return err
	}

	// Copy the current file permissions to the new binary and ensure it is executable.
	err = os.Chmod(new, oldInfo.Mode())
	if err != nil {
		return err
	}

	// Backup only for upgrading, not when reverting which is assumed if the new binary ends in `.bak`.
	if !strings.HasSuffix(new, ".bak") {
		// Backup the current binary, so that it may be restored via `roachprod update --revert`.
		err = os.Rename(old, old+".bak")
		if err != nil {
			return errors.WithDetail(err, "unable to backup current binary")
		}
	}

	// Move the new binary into place.
	return os.Rename(new, old)
}

// Computes the age of the current binary, relative to the given update time.
func TimeSinceUpdate(updateTime time.Time) (time.Duration, error) {
	currentBinary, err := os.Executable()
	if err != nil {
		return -1, err
	}
	statInfo, err := os.Stat(currentBinary)
	if err != nil {
		return -1, err
	}
	return updateTime.Sub(statInfo.ModTime()), nil
}

// Provide `cobra.Command` functions with a standard return code handler.
// Exit codes come from rperrors.Error.ExitCode().
//
// If the wrapped error tree of an error does not contain an instance of
// rperrors.Error, the error will automatically be wrapped with
// rperrors.Unclassified.
func wrap(f func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		var err error
		isSecure, err = isSecureCluster(cmd)
		if err != nil {
			cmd.Printf("Error: %v\n", err)
			os.Exit(1)
		}

		err = f(cmd, args)
		if err != nil {
			roachprodError, ok := rperrors.AsError(err)
			if !ok {
				roachprodError = rperrors.Unclassified{Err: err}
				err = roachprodError
			}

			cmd.Printf("Error: %+v\n", err)

			os.Exit(roachprodError.ExitCode())
		}
	}
}

func isSecureCluster(cmd *cobra.Command) (bool, error) {
	hasSecureFlag := cmd.Flags().Changed("secure")
	hasInsecureFlag := cmd.Flags().Changed("insecure")

	switch {
	case hasSecureFlag && hasInsecureFlag:
		// Disallow passing both flags, even if they are consistent.
		return false, fmt.Errorf("cannot pass both --secure and --insecure flags")

	case hasSecureFlag:
		desc := "Clusters are secure by default"
		if !secure {
			desc = "Use the --insecure flag to create insecure clusters"
		}

		fmt.Printf("WARNING: --secure flag is deprecated. %s.\n", desc)
		return secure, nil

	default:
		return !insecure, nil
	}
}

func printPublicKeyTable(keys gce.AuthorizedKeys, includeSize bool) error {
	// Align columns left and separate with at least two spaces.
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)

	fmt.Fprintf(tw, "%s\t%s\n", "User", "Key")
	for _, ak := range keys {
		fmt.Fprintf(tw, "%s\t%s\n", ak.User, ak.Format(64 /* maxLen */))
	}

	err := tw.Flush()
	if !includeSize {
		return err
	}

	const maxProjectMetadataBytes = 262144 /* 256 KiB */
	metadataLen := len(keys.AsProjectMetadata())

	usage := int(float64(metadataLen*100) / float64(maxProjectMetadataBytes))
	_, err = fmt.Printf("\nTOTAL: %d bytes (usage: %d%%)\n", metadataLen, usage)
	return err
}

// addHelpAboutNodes adds help about nodes to each of the commands
func addHelpAboutNodes(cmd *cobra.Command) {
	// Add help about specifying nodes
	if cmd.Long == "" {
		cmd.Long = cmd.Short
	}
	cmd.Long += fmt.Sprintf(`
Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod %[1]s marc-test:1-3,8-9

  will perform %[1]s on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9
`, cmd.Name())
}

// Before executing any command, validate and canonicalize args.
func ValidateAndConfigure(cmd *cobra.Command, args []string) {
	// Skip validation for commands that are self-sufficient.
	switch cmd.Name() {
	case "help", "version", "list":
		return
	}

	printErrAndExit := func(err error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}

	// Validate architecture flag, if set.
	if archOpt := cmd.Flags().Lookup("arch"); archOpt != nil && archOpt.Changed {
		arch := vm.CPUArch(strings.ToLower(archOpt.Value.String()))

		if arch != vm.ArchAMD64 && arch != vm.ArchARM64 && arch != vm.ArchFIPS {
			printErrAndExit(fmt.Errorf("unsupported architecture %q", arch))
		}
		if string(arch) != archOpt.Value.String() {
			// Set the canonical value.
			_ = cmd.Flags().Set("arch", string(arch))
		}
	}

	// Validate cloud providers, if set.
	providersSet := make(map[string]struct{})
	for _, p := range createVMOpts.VMProviders {
		if _, ok := vm.Providers[p]; !ok {
			printErrAndExit(fmt.Errorf("unknown cloud provider %q", p))
		}
		if _, ok := providersSet[p]; ok {
			printErrAndExit(fmt.Errorf("duplicate cloud provider specified %q", p))
		}
		providersSet[p] = struct{}{}
	}
}
