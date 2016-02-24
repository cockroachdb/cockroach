package main

import (
	"fmt"
	"go/build"
)

var cmdCmd = &Command{
	UsageLine: "cmd [project import path] [cmd import path]",
	Short:     "add a command to your project's GLOCKFILE",
	Long: `cmd is used to record a Go command-line tool that the project depends on.

The cmd import path must reference a main package. Its dependencies will be
included in the GLOCKFILE along with the project's dependencies, and the command
will be built by "glock sync" and updated by "glock apply".

This functionality allows you to install and update development tools like "vet"
and "errcheck" across a team.  It can even be used to automatically update glock
itself.

Commands are recorded at the top of your GLOCKFILE, in the following format:

	cmd code.google.com/p/go.tools/cmd/godoc
	cmd code.google.com/p/go.tools/cmd/goimports
	cmd code.google.com/p/go.tools/cmd/vet

Dependencies of the commands will be included in the overall calculation and
included alongside your project's library dependencies within the GLOCKFILE.

Options:

	-n	print to stdout instead of writing to file.

`,
}

var cmdN = cmdCmd.Flag.Bool("n", false, "Don't save the file, just print to stdout")

func init() {
	cmdCmd.Run = runCmd // break init loop
}

func runCmd(_ *Command, args []string) {
	if len(args) != 2 {
		cmdCmd.Usage()
		return
	}

	var (
		importPath = args[0]
		cmd        = args[1]
	)

	// Import the cmd, verify it's a main package, and build it.
	pkg, err := build.Import(cmd, "", 0)
	if err != nil {
		perror(fmt.Errorf("Failed to import %v: %v", cmd, err))
	}
	if pkg.Name != "main" {
		perror(fmt.Errorf("Found package %v, expected main", pkg.Name))
	}
	installOutput, err := run("go", "install", "-v", cmd)
	if err != nil {
		perror(fmt.Errorf("Failed to build %v:\n%v", cmd, string(installOutput)))
	}

	// Add new cmd to the list, recalculate dependencies, and write result
	var (
		cmds     = append(readCmds(importPath), cmd)
		depRoots = calcDepRoots(importPath, cmds)
		output   = glockfileWriter(importPath, *cmdN)
	)
	outputCmds(output, cmds)
	outputDeps(output, depRoots)
	output.Close()
}
