// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/build/starlarkutil"
	"github.com/google/skylark/syntax"
)

type sourceFiles struct {
	depsBzl, workspace, cdepsArchivedBzl, cdepsRepositoriesBzl string
}

func getSourceFiles() (*sourceFiles, error) {
	depsBzl, err := bazel.Runfile("DEPS.bzl")
	if err != nil {
		return &sourceFiles{}, err
	}
	workspace, err := bazel.Runfile("WORKSPACE")
	if err != nil {
		return &sourceFiles{}, err
	}
	cdepsArchivedBzl, err := bazel.Runfile("c-deps/archived.bzl")
	if err != nil {
		return &sourceFiles{}, err
	}
	cdepsRepositoriesBzl, err := bazel.Runfile("c-deps/REPOSITORIES.bzl")
	if err != nil {
		return &sourceFiles{}, err
	}
	return &sourceFiles{depsBzl, workspace, cdepsArchivedBzl, cdepsRepositoriesBzl}, nil
}

func getShasFromDepsBzl(depsBzl string, shas map[string]string) error {
	artifacts, err := starlarkutil.ListArtifactsInDepsBzl(depsBzl)
	if err != nil {
		return err
	}
	for _, artifact := range artifacts {
		shas[artifact.URL] = artifact.Sha256
	}
	return nil
}

func getShasFromGoDownloadSdkCall(call *syntax.CallExpr, shas map[string]string) error {
	var urlTmpl, version string
	baseToHash := make(map[string]string)
	for _, arg := range call.Args {
		switch bx := arg.(type) {
		case *syntax.BinaryExpr:
			if bx.Op != syntax.EQ {
				return fmt.Errorf("unexpected binary expression Op %d", bx.Op)
			}
			kwarg, err := starlarkutil.ExpectIdent(bx.X)
			if err != nil {
				return err
			}
			if kwarg == "sdks" {
				switch d := bx.Y.(type) {
				case *syntax.DictExpr:
					for _, ex := range d.List {
						switch entry := ex.(type) {
						case *syntax.DictEntry:
							strs, err := starlarkutil.ExpectTupleOfStrings(entry.Value, 2)
							if err != nil {
								return err
							}
							baseToHash[strs[0]] = strs[1]
						default:
							return fmt.Errorf("expected DictEntry in dictionary expression")
						}
					}
				default:
					return fmt.Errorf("expected dict as value of sdks kwarg")
				}
			} else if kwarg == "urls" {
				urlTmpl, err = starlarkutil.ExpectSingletonStringList(bx.Y)
				if err != nil {
					return err
				}
			} else if kwarg == "version" {
				version, err = starlarkutil.ExpectLiteralString(bx.Y)
				if err != nil {
					return err
				}
			}
		}
	}
	if urlTmpl == "" || version == "" {
		return fmt.Errorf("expected both `urls` and `version` to be set")
	}
	for basename, sha := range baseToHash {
		shas[strings.ReplaceAll(urlTmpl, "{}", basename)] = sha
	}
	return nil
}

func getShasFromNodeRepositoriesCall(call *syntax.CallExpr, shas map[string]string) error {
	var nodeURLTmpl, nodeVersion string
	nodeBaseToHash := make(map[string]string)
	for _, arg := range call.Args {
		switch bx := arg.(type) {
		case *syntax.BinaryExpr:
			if bx.Op != syntax.EQ {
				return fmt.Errorf("unexpected binary expression Op %d", bx.Op)
			}
			kwarg, err := starlarkutil.ExpectIdent(bx.X)
			if err != nil {
				return err
			}
			if kwarg == "node_repositories" {
				switch d := bx.Y.(type) {
				case *syntax.DictExpr:
					for _, ex := range d.List {
						switch entry := ex.(type) {
						case *syntax.DictEntry:
							strs, err := starlarkutil.ExpectTupleOfStrings(entry.Value, 3)
							if err != nil {
								return err
							}
							nodeBaseToHash[strs[0]] = strs[2]
						default:
							return fmt.Errorf("expected DictEntry in dictionary expression")
						}
					}
				default:
					return fmt.Errorf("expected dict as value of node_repositories kwarg")
				}
			} else if kwarg == "node_urls" {
				nodeURLTmpl, err = starlarkutil.ExpectSingletonStringList(bx.Y)
				if err != nil {
					return err
				}
			} else if kwarg == "node_version" {
				nodeVersion, err = starlarkutil.ExpectLiteralString(bx.Y)
				if err != nil {
					return err
				}
			}
		}
	}
	if nodeURLTmpl == "" || nodeVersion == "" {
		return fmt.Errorf("did not parse all needed data from node_repositories call")
	}
	for base, sha := range nodeBaseToHash {
		shas[strings.ReplaceAll(strings.ReplaceAll(nodeURLTmpl, "{version}", nodeVersion), "{filename}", base)] = sha
	}
	return nil
}

func getShasFromWorkspace(workspace string, shas map[string]string) error {
	in, err := os.Open(workspace)
	if err != nil {
		return err
	}
	defer in.Close()
	parsed, err := syntax.Parse("WORKSPACE", in, 0)
	if err != nil {
		return err
	}
	for _, stmt := range parsed.Stmts {
		switch s := stmt.(type) {
		case *syntax.ExprStmt:
			switch x := s.X.(type) {
			case *syntax.CallExpr:
				fun := starlarkutil.GetFunctionFromCall(x)
				if fun == "http_archive" {
					artifact, err := starlarkutil.GetArtifactFromHTTPArchive(x)
					if err != nil {
						return err
					}
					shas[artifact.URL] = artifact.Sha256
				}
				if fun == "go_repository" {
					artifact, err := starlarkutil.GetArtifactFromGoRepository(x)
					if err != nil {
						return err
					}
					shas[artifact.URL] = artifact.Sha256
				}
				if fun == "go_download_sdk" {
					if err := getShasFromGoDownloadSdkCall(x, shas); err != nil {
						return err
					}
				}
				if fun == "node_repositories" {
					if err := getShasFromNodeRepositoriesCall(x, shas); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

type archivedCdep struct {
	lib, config, sha string
}

func getShasFromArchivedCdeps(archivedBzl, repositoriesBzl string, shas map[string]string) error {
	archived, err := os.Open(archivedBzl)
	if err != nil {
		return err
	}
	defer archived.Close()
	parsed, err := syntax.Parse("archived.bzl", archived, 0)
	if err != nil {
		return err
	}
	var cdepURLTmpl, loc string
	for _, stmt := range parsed.Stmts {
		switch a := stmt.(type) {
		case *syntax.AssignStmt:
			name, err := starlarkutil.ExpectIdent(a.LHS)
			if err != nil {
				return err
			}
			if name == "URL_TMPL" {
				cdepURLTmpl, err = starlarkutil.ExpectLiteralString(a.RHS)
				if err != nil {
					return err
				}
			} else if name == "LOC" {
				loc, err = starlarkutil.ExpectLiteralString(a.RHS)
				if err != nil {
					return err
				}
			}
		}
	}
	repositories, err := os.Open(repositoriesBzl)
	if err != nil {
		return err
	}
	defer repositories.Close()
	parsed, err = syntax.Parse("REPOSITORIES.bzl", repositories, 0)
	if err != nil {
		return err
	}
	var archivedCdeps []*archivedCdep
	for _, stmt := range parsed.Stmts {
		switch def := stmt.(type) {
		case *syntax.DefStmt:
			if def.Name.Name == "c_deps" {
				for _, subStmt := range def.Function.Body {
					switch s := subStmt.(type) {
					case *syntax.ExprStmt:
						switch x := s.X.(type) {
						case *syntax.CallExpr:
							cdep, err := maybeGetArchivedCdep(x)
							if err != nil {
								return err
							}
							if cdep != nil {
								archivedCdeps = append(archivedCdeps, cdep)
							}
						}
					}
				}
			}
		}
	}
	for _, cdep := range archivedCdeps {
		url := strings.ReplaceAll(cdepURLTmpl, "{loc}", loc)
		url = strings.ReplaceAll(url, "{lib}", cdep.lib)
		url = strings.ReplaceAll(url, "{config}", cdep.config)
		shas[url] = cdep.sha
	}
	return nil
}

// Returns an archivedCdep or nil if this call is not an archived_cdep_repository call.
func maybeGetArchivedCdep(call *syntax.CallExpr) (*archivedCdep, error) {
	fun := starlarkutil.GetFunctionFromCall(call)
	if fun == "archived_cdep_repository" {
		var lib, config, sha string
		for _, arg := range call.Args {
			switch bx := arg.(type) {
			case *syntax.BinaryExpr:
				if bx.Op != syntax.EQ {
					return nil, fmt.Errorf("unexpected binary expression Op %d", bx.Op)
				}
				kwarg, err := starlarkutil.ExpectIdent(bx.X)
				if err != nil {
					return nil, err
				}
				if kwarg == "lib" {
					lib, err = starlarkutil.ExpectLiteralString(bx.Y)
					if err != nil {
						return nil, err
					}
				} else if kwarg == "config" {
					config, err = starlarkutil.ExpectLiteralString(bx.Y)
					if err != nil {
						return nil, err
					}
				} else if kwarg == "sha256" {
					sha, err = starlarkutil.ExpectLiteralString(bx.Y)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		return &archivedCdep{lib, config, sha}, nil
	}
	return nil, nil
}

// getShas returns a map of URL -> SHA256 for each artifact.
func getShas(src *sourceFiles) (map[string]string, error) {
	ret := make(map[string]string)
	if err := getShasFromDepsBzl(src.depsBzl, ret); err != nil {
		return nil, err
	}
	if err := getShasFromWorkspace(src.workspace, ret); err != nil {
		return nil, err
	}
	if err := getShasFromArchivedCdeps(src.cdepsArchivedBzl, src.cdepsRepositoriesBzl, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func dumpOutput(shas map[string]string) {
	bases := make(map[string]interface{})
	for url := range shas {
		base := path.Base(url)
		_, ok := bases[base]
		if ok {
			panic("cannot have two files with the same basename in DISTDIR_FILES")
		}
		bases[base] = nil
	}

	fmt.Println(`# Code generated by generate-distdir. DO NOT EDIT.

DISTDIR_FILES = {`)
	urls := make([]string, 0, len(shas))
	for url := range shas {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	for _, url := range urls {
		fmt.Printf(`    "%s": "%s",
`, url, shas[url])
	}
	fmt.Println("}")
}

func generateDistdir() error {
	src, err := getSourceFiles()
	if err != nil {
		return err
	}
	shas, err := getShas(src)
	if err != nil {
		return err
	}
	dumpOutput(shas)
	return nil
}

func main() {
	if err := generateDistdir(); err != nil {
		panic(err)
	}
}
