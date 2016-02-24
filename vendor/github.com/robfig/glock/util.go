package main

import (
	"fmt"
	"go/build"
	"io"
	"os"
	"path"
	"path/filepath"
)

// managedRepo is the repo being managed by glock (where the GLOCKFILE resides)
// Like repoRoot, except managedRepo is allowed to be outside the GOPATH.
type managedRepo struct {
	vcs *vcsCmd
	dir string
}

// managedRepoRoot finds the VCS root for a given import path.  Unlike
// dependency repos, the source repo is allowed to have its root outside of the
// GOPATH. This supports the use case of mounting your GOPATH within a
// repository, common in polyglot repos.
func managedRepoRoot(importPath string) (*managedRepo, error) {
	var pkg, err = build.Import(importPath, "", build.FindOnly)
	if err != nil {
		return nil, err
	}

	var dir = pkg.Dir
	for len(dir) > 1 {
		vcs, err := lookVCS(dir)
		if err == nil {
			return &managedRepo{vcs, dir}, nil
		}
		dir = filepath.Dir(dir)
	}
	return nil, fmt.Errorf("no version control directory found for %q", importPath)
}

// glockRepoRootForImportPath wraps the vcs.go version.  If the stock one
// doesn't work, it looks for .git, .hg directories up to the tree.
// This is done to support repos with non-go-get friendly names.
// Also, returns an error if it doesn't exist (e.g. it needs to be go gotten).
func glockRepoRootForImportPath(importPath string) (*repoRoot, error) {
	var pkg, err = build.Import(importPath, "", build.FindOnly)
	if err != nil {
		return nil, err
	}

	rr, err := repoRootForImportPath(importPath)
	if err == nil {
		// it may not exist, even with err == nil
		_, err = os.Stat(pkg.Dir)
		if err != nil {
			return nil, err
		}
		return rr, nil
	}

	var dir = pkg.ImportPath
	for len(dir) > 1 {
		rr, err := fastRepoRoot(dir)
		if err == nil {
			return rr, nil
		}
		dir = filepath.Dir(dir)
	}

	return nil, fmt.Errorf("no version control directory found for %q", importPath)
}

// fastRepoRoot just checks for existence of VCS dot directories to determine
// which VCS to use for the given import path.
// If none are found, an error is returned.
func fastRepoRoot(rootImportPath string) (*repoRoot, error) {
	var pkg, err = build.Import(rootImportPath, "", build.FindOnly)
	if err != nil {
		return nil, err
	}

	vcs, err := lookVCS(pkg.Dir)
	if err != nil {
		return nil, err
	}

	return &repoRoot{
		vcs:  vcs,
		repo: "",
		root: rootImportPath,
	}, nil
}

// lookVCS looks for known VCS dot directories in the given directory, and
// returns a vcs cmd if found, or an error if not (or if an error was encountered).
func lookVCS(dir string) (*vcsCmd, error) {
	for _, vcsDir := range []string{".git", ".hg", ".bzr", ".svn"} {
		_, err := os.Stat(filepath.Join(dir, vcsDir))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		vcs := vcsByCmd(vcsDir[1:])
		if vcs == nil {
			return nil, fmt.Errorf("unknown version control system %q", vcsDir[1:])
		}
		return vcs, nil
	}
	return nil, fmt.Errorf("no repo found: %s", dir)
}

func gopath() string {
	return filepath.SplitList(build.Default.GOPATH)[0]
}

func glockFilename(importPath string) string {
	return path.Join(gopath(), "src", importPath, "GLOCKFILE")
}

func glockfileReader(importPath string, n bool) io.ReadCloser {
	if n {
		return os.Stdin
	}

	var glockfile, err = os.Open(glockFilename(importPath))
	if err != nil {
		perror(err)
	}
	return glockfile
}

func glockfileWriter(importPath string, n bool) io.WriteCloser {
	if n {
		return os.Stdout
	}

	var f, err = os.Create(glockFilename(importPath))
	if err != nil {
		perror(fmt.Errorf("error creating %s: %v", glockFilename, err))
	}
	return f
}
