package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

var dataDirectory = flag.String("data_directory", "data/", "cockroach data directory")
var fillRatio = flag.Float64("fill_ratio", 0.0, "fraction of cockroach data "+
	"directory to be filled")

func main() {
	flag.Parse()

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
}
