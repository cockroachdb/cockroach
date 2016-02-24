package main

import (
	"bufio"
	"fmt"
	"go/build"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var cmdApply = &Command{
	UsageLine: "apply [import path]",
	Short:     "apply the changes described by a GLOCKFILE diff (on STDIN) to the current GOPATH.",
	Long: `apply the changes described by a GLOCKFILE diff (on STDIN) to the current GOPATH.

It is meant to be called from a VCS hook on any change to the GLOCKFILE.
`,
}

func init() {
	cmdApply.Run = runApply // break init loop
}

var actionstr = map[action]string{
	add:    "add    ",
	update: "update ",
	remove: "remove ",
}

func runApply(cmd *Command, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "error: no import path provided\n")
		cmdApply.Usage()
		return
	}
	var importPath = args[0]
	var gopath = filepath.SplitList(build.Default.GOPATH)[0]
	var book = buildPlaybook(readDiffLines(os.Stdin))

	var updated = false
	for _, cmd := range book.library {
		fmt.Printf("%s %-50.49s %s\n", actionstr[cmd.action], cmd.importPath, cmd.revision)
		var importDir = path.Join(gopath, "src", cmd.importPath)
		switch cmd.action {
		case remove:
			// do nothing
		case update:
			updated = true
			fallthrough
		case add:
			// add or update the dependency
			run("go", "get", "-u", "-d", path.Join(cmd.importPath, "..."))

			// update that dependency
			var repo, err = repoRootForImportPath(cmd.importPath)
			if err != nil {
				fmt.Println("error determining repo root for", cmd.importPath, err)
				continue
			}
			err = repo.vcs.run(importDir, repo.vcs.tagSyncCmd, "tag", cmd.revision)
			if err != nil {
				fmt.Println("error syncing", cmd.importPath, "to", cmd.revision, "-", err)
				continue
			}
		}
	}

	// Collect the import paths for all added commands.
	var cmds []string
	for _, cmd := range book.cmd {
		if cmd.add {
			cmds = append(cmds, cmd.importPath)
		}
	}

	// If a package was updated, reinstall all commands.
	if updated {
		cmds = nil
		var glockfile = glockfileReader(importPath, false)
		var scanner = bufio.NewScanner(glockfile)
		for scanner.Scan() {
			var txt = scanner.Text()
			if strings.HasPrefix(txt, "cmd ") {
				cmds = append(cmds, txt[4:])
			}
		}
		if err := scanner.Err(); err != nil {
			perror(err)
		}
	}

	for _, cmd := range cmds {
		fmt.Println("install", cmd)
		installOutput, err := run("go", "install", cmd)
		if err != nil {
			fmt.Println("failed:\n", string(installOutput), err)
		}
	}
}
