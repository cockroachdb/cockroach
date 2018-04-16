package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var rootFlags = pflag.NewFlagSet(`run`, pflag.ExitOnError)
var dataDirectory = rootFlags.String("data_directory", "data/", "cockroach data directory")
var fillRatio = rootFlags.Float64("fill_ratio", 0.0, "fraction of cockroach data "+
	"directory to be filled")

var rootCmd = &cobra.Command{
	Use:   "fill_ballast",
	Short: "Fill the cockroach data directory with given ballast file",
	Run: func(cmd *cobra.Command, args []string) {
		fs := syscall.Statfs_t{}
		err := syscall.Statfs(*dataDirectory, &fs)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		total := fs.Blocks * uint64(fs.Bsize)
		free := fs.Bavail * uint64(fs.Bsize)
		used := total - free
		toFill := uint64((*fillRatio) * float64(total))
		fmt.Println(total)
		fmt.Println(free)
		fmt.Println(used)
		fmt.Println(toFill)
		if used > toFill {
			fmt.Printf("Used space %v already more than needed to be filled %v\n", used, toFill)
			os.Exit(1)
		}
		if used == toFill {
			return
		}
		ballastFile := filepath.Join(*dataDirectory, "ballast_file_to_fill_store")
		ballastSize := toFill - used
		createBallastCommand := exec.Command("fallocate", "-l", fmt.Sprint(ballastSize), ballastFile)
		if err := createBallastCommand.Run(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.Flags().AddFlagSet(rootFlags)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
