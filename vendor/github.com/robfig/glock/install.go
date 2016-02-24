package main

import (
	"fmt"
	"go/build"
	"io/ioutil"
	"os"
	"path/filepath"
)

var cmdInstall = &Command{
	UsageLine: "install [import path]",
	Short:     "add a post-merge hook that applies GLOCKFILE changes after each pull.",
	Long: `Install adds a glock hook to the given package's repository

When pulling new commits, it checks whether the GLOCKFILE has been updated. If so,
it calls "glock apply", passing in the diff.`,
}

func init() {
	cmdInstall.Run = runInstall // break init loop
}

const gitHook = `#!/usr/bin/env bash
set -e

[[ ! $GIT_REFLOG_ACTION =~ %v ]] && exit 0

LOG=$(git log -U0 --oneline -p HEAD@{1}..HEAD %s)
[ -z "$LOG" ] && echo "glock: no changes to apply" && exit 0
echo "glock: applying updates..."
glock apply %s <<< "$LOG"
`

type hook struct{ filename, content, action string }

var vcsHooks = map[*vcsCmd][]hook{
	vcsGit: {
		{filepath.Join(".git", "hooks", "post-merge"), gitHook, "pull"},
		{filepath.Join(".git", "hooks", "post-checkout"), gitHook, "pull[[:space:]]+--rebase"},
		{filepath.Join(".git", "hooks", "post-rewrite"), gitHook, "rebase"},
	},
}

func runInstall(cmd *Command, args []string) {
	if len(args) == 0 {
		cmdInstall.Usage()
		return
	}
	var importPath = args[0]
	var repo, err = managedRepoRoot(importPath)
	if err != nil {
		perror(err)
	}
	var hooks, ok = vcsHooks[repo.vcs]
	if !ok {
		perror(fmt.Errorf("%s hook not implemented", repo.vcs.name))
	}

	var glockfilePath = calcGlockfilePath(importPath, repo)
	for _, hook := range hooks {
		var filename = filepath.Join(repo.dir, hook.filename)
		var err = os.MkdirAll(filepath.Dir(filename), 0755)
		if err != nil {
			perror(err)
		}
		var hookContent = fmt.Sprintf(hook.content, hook.action, glockfilePath, importPath)
		err = ioutil.WriteFile(filename, []byte(hookContent), 0755)
		if err != nil {
			perror(err)
		}
		fmt.Println("Installed", filename)
	}
}

// calcGlockfilePath calculates the relative path to the GLOCKFILE from the root
// of the repo.
func calcGlockfilePath(importPath string, repo *managedRepo) string {
	var pkg, err = build.Import(importPath, "", build.FindOnly)
	if err != nil {
		perror(err)
	}

	var relPath = ""
	if len(repo.dir) < len(pkg.Dir) {
		relPath = pkg.Dir[len(repo.dir)+1:]
	}

	return filepath.Join(relPath, "GLOCKFILE")
}
