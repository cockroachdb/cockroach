// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/build/starlarkutil"
	"github.com/google/skylark/syntax"
)

type sourceFiles struct {
	depsBzl, workspace string
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
	return &sourceFiles{depsBzl, workspace}, nil
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
	var nodeURLTmpl, yarnURLTmpl, nodeVersion, yarnVersion, yarnSha, yarnFilename string
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
			} else if kwarg == "yarn_repositories" {
				switch d := bx.Y.(type) {
				case *syntax.DictExpr:
					if len(d.List) != 1 {
						return fmt.Errorf("expected only one version in yarn_repositories dict")
					}
					switch entry := d.List[0].(type) {
					case *syntax.DictEntry:
						strs, err := starlarkutil.ExpectTupleOfStrings(entry.Value, 3)
						if err != nil {
							return err
						}
						yarnFilename = strs[0]
						yarnSha = strs[2]
					default:
						return fmt.Errorf("expected DictEntry in dictionary expression")
					}
				default:
					return fmt.Errorf("expected dict as value of yarn_repositories kwarg")
				}
			} else if kwarg == "yarn_urls" {
				yarnURLTmpl, err = starlarkutil.ExpectSingletonStringList(bx.Y)
				if err != nil {
					return err
				}
			} else if kwarg == "yarn_version" {
				yarnVersion, err = starlarkutil.ExpectLiteralString(bx.Y)
				if err != nil {
					return err
				}
			}
		}
	}
	if nodeURLTmpl == "" || yarnURLTmpl == "" || nodeVersion == "" || yarnVersion == "" || yarnSha == "" || yarnFilename == "" {
		return fmt.Errorf("did not parse all needed data from node_repositories call")
	}
	for base, sha := range nodeBaseToHash {
		shas[strings.ReplaceAll(strings.ReplaceAll(nodeURLTmpl, "{version}", nodeVersion), "{filename}", base)] = sha
	}
	shas[strings.ReplaceAll(strings.ReplaceAll(yarnURLTmpl, "{version}", yarnVersion), "{filename}", yarnFilename)] = yarnSha
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

// getShas returns a map of URL -> SHA256 for each artifact.
func getShas(src *sourceFiles) (map[string]string, error) {
	ret := make(map[string]string)
	if err := getShasFromDepsBzl(src.depsBzl, ret); err != nil {
		return nil, err
	}
	if err := getShasFromWorkspace(src.workspace, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func dumpOutput(shas map[string]string) {
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
