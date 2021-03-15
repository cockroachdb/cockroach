package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/directory"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/spf13/cobra"
)

var testDirectoryOptions directory.TestDirectoryOptions

func init() {
	f := mtTestDirectorySvr.Flags()
	ServerPort := cliflags.FlagInfo{
		Name:        "port",
		Description: `Port to listen on.`,
	}
	intFlag(f, &testDirectoryOptions.Port, ServerPort)
}

var mtTestDirectorySvr = &cobra.Command{
	Use:   "test-directory",
	Short: "Run a test directory service.",
	Long: `
Run a test directory service.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runDirectorySvr),
}

func runDirectorySvr(cmd *cobra.Command, args []string) (returnErr error) {
	ctx := context.Background()
	serverCfg.Stores.Specs = nil

	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	return directory.RunTestDirectory(testDirectoryOptions)
}
