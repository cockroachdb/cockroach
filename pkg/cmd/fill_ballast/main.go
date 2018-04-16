package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

var ballastFile = flag.String("ballast_file", "data/ballast_file_to_fill_store", "ballast file in cockroach data directory")
var fillRatio = flag.Float64("fill_ratio", 0.0, "fraction of cockroach data "+
	"directory to be filled")
var diskLeftInBytes = flag.Uint64("disk_left_bytes", 0, "amount of cockroach data "+
	"directory to be left empty")

func main() {
	flag.Parse()
	if *fillRatio < 0 {
		fmt.Println("fill_ratio expected to be positive, got: ", *fillRatio)
		os.Exit(1)
	}
	if *diskLeftInBytes < 0 {
		fmt.Println("disk_left_bytes expected to be positive, got: ", *diskLeftInBytes)
		os.Exit(1)
	}
	if *fillRatio > 0 && *diskLeftInBytes > 0 {
		fmt.Printf(
			"fill_ratio or disk_left_bytes expected to be positive, " +
				"got: fill_ratio: %v, disk_left_bytesv: %v\n",
				*fillRatio,
				*diskLeftInBytes,
		)
		os.Exit(1)
	}
	dataDirectory := filepath.Dir(*ballastFile)
	fs := syscall.Statfs_t{}

	err := syscall.Statfs(dataDirectory, &fs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	total := fs.Blocks * uint64(fs.Bsize)
	free := fs.Bavail * uint64(fs.Bsize)
	used := total - free
	var toFill uint64
	if *fillRatio > 0 {
		toFill = uint64((*fillRatio) * float64(total))
	}
	if *diskLeftInBytes > 0 {
		toFill = total - *diskLeftInBytes
	}
	if used > toFill {
		fmt.Printf("Used space %v already more than needed to be filled %v\n", used, toFill)
		os.Exit(1)
	}
	if used == toFill {
		return
	}
	ballastSize := toFill - used
	createBallastCommand := exec.Command("fallocate", "-l", fmt.Sprint(ballastSize), *ballastFile)
	if err := createBallastCommand.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
