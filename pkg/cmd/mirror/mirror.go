// Copyright 2021 The Cockroach Authors.
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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/google/skylark/syntax"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
)

const gcpBucket = "cockroach-godeps"

// downloadedModule captures `go mod download -json` output.
type downloadedModule struct {
	Path    string `json:"Path"`
	Sum     string `json:"Sum"`
	Version string `json:"Version"`
	Zip     string `json:"Zip"`
}

// listedModule captures `go list -m -json` output.
type listedModule struct {
	Path    string        `json:"Path"`
	Version string        `json:"Version"`
	Replace *listedModule `json:"Replace,omitempty"`
}

type existingMirror struct {
	url    string
	sha256 string
}

func canMirror() bool {
	return envutil.EnvOrDefaultBool("COCKROACH_BAZEL_CAN_MIRROR", false)
}

func formatSubURL(path, version string) string {
	return fmt.Sprintf("gomod/%s/%s-%s.zip", path, modulePathToBazelRepoName(path), version)
}

func formatURL(path, version string) string {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s",
		gcpBucket, formatSubURL(path, version))
}

func getSha256OfFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func uploadFile(ctx context.Context, client *storage.Client, localPath, remotePath string) error {
	in, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", localPath, err)
	}
	defer in.Close()
	out := client.Bucket(gcpBucket).Object(remotePath).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusPreconditionFailed {
				// In this case the "DoesNotExist" precondition
				// failed, i.e., the object does already exist.
				return nil
			}
			return gerr
		}
		return err
	}
	return nil
}

func createTmpDir() (tmpdir string, err error) {
	tmpdir, err = bazel.NewTmpDir("gomirror")
	if err != nil {
		return
	}
	gomod, err := bazel.Runfile("go.mod")
	if err != nil {
		return
	}
	gosum, err := bazel.Runfile("go.sum")
	if err != nil {
		return
	}
	err = copyFile(gomod, filepath.Join(tmpdir, "go.mod"))
	if err != nil {
		return
	}
	err = copyFile(gosum, filepath.Join(tmpdir, "go.sum"))
	return
}

func downloadZips(tmpdir string) (map[string]downloadedModule, error) {
	gobin, err := bazel.Runfile("bin/go")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(gobin, "mod", "download", "-json")
	cmd.Dir = tmpdir
	jsonBytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var jsonBuilder strings.Builder
	ret := make(map[string]downloadedModule)
	for _, line := range strings.Split(string(jsonBytes), "\n") {
		jsonBuilder.WriteString(line)
		if strings.HasPrefix(line, "}") {
			var mod downloadedModule
			if err := json.Unmarshal([]byte(jsonBuilder.String()), &mod); err != nil {
				return nil, err
			}
			ret[mod.Path] = mod
			jsonBuilder.Reset()
		}
	}
	return ret, nil
}

func listAllModules(tmpdir string) (map[string]listedModule, error) {
	gobin, err := bazel.Runfile("bin/go")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(gobin, "list", "-mod=readonly", "-m", "-json", "all")
	cmd.Dir = tmpdir
	jsonBytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	ret := make(map[string]listedModule)
	var jsonBuilder strings.Builder
	for _, line := range strings.Split(string(jsonBytes), "\n") {
		jsonBuilder.WriteString(line)
		if strings.HasPrefix(line, "}") {
			var mod listedModule
			if err := json.Unmarshal([]byte(jsonBuilder.String()), &mod); err != nil {
				return nil, err
			}
			jsonBuilder.Reset()
			// The output will include the `cockroach` module, but we
			// can just throw it away.
			if mod.Path == "github.com/cockroachdb/cockroach" {
				continue
			}
			ret[mod.Path] = mod
		}
	}
	return ret, nil
}

func getExistingMirrors() (map[string]existingMirror, error) {
	depsbzl, err := bazel.Runfile("DEPS.bzl")
	if err != nil {
		return nil, err
	}
	in, err := os.Open(depsbzl)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	return getExistingMirrorsFromDepsBzl(in)
}

func getExistingMirrorsFromDepsBzl(in interface{}) (map[string]existingMirror, error) {
	parsed, err := syntax.Parse("DEPS.bzl", in, 0)
	if err != nil {
		return nil, err
	}
	for _, stmt := range parsed.Stmts {
		switch s := stmt.(type) {
		case *syntax.DefStmt:
			if s.Name.Name == "go_deps" {
				return existingMirrorsFromGoDeps(s)
			}
		default:
			continue
		}
	}
	return nil, fmt.Errorf("could not find go_deps function in DEPS.bzl")
}

func existingMirrorsFromGoDeps(def *syntax.DefStmt) (map[string]existingMirror, error) {
	ret := make(map[string]existingMirror)
	for _, stmt := range def.Function.Body {
		switch s := stmt.(type) {
		case *syntax.ExprStmt:
			switch x := s.X.(type) {
			case *syntax.CallExpr:
				name, mirror, err := maybeGetExistingMirror(x)
				if err != nil {
					return nil, err
				}
				if name != "" {
					ret[name] = mirror
				}
			default:
				return nil, fmt.Errorf("unexpected expression in DEPS.bzl: %v", x)
			}
		}
	}
	return ret, nil
}

// maybeGetExistingMirror returns the existing mirror pointed to by the given
// go_repository expression, returning the name of the repo and the location of
// the mirror if one can be found, or the empty string/an empty existingMirror
// if not. Returns an error iff an unrecoverable problem occurred.
func maybeGetExistingMirror(call *syntax.CallExpr) (string, existingMirror, error) {
	fn, err := expectIdent(call.Fn)
	if err != nil {
		return "", existingMirror{}, err
	}
	if fn != "go_repository" {
		return "", existingMirror{}, fmt.Errorf("expected go_repository, got %s", fn)
	}
	var name, sha256, url string
	for _, arg := range call.Args {
		switch bx := arg.(type) {
		case *syntax.BinaryExpr:
			if bx.Op != syntax.EQ {
				return "", existingMirror{}, fmt.Errorf("unexpected binary expression Op %d", bx.Op)
			}
			kwarg, err := expectIdent(bx.X)
			if err != nil {
				return "", existingMirror{}, err
			}
			if kwarg == "name" {
				name, err = expectLiteralString(bx.Y)
				if err != nil {
					return "", existingMirror{}, err
				}
			}
			if kwarg == "sha256" {
				sha256, err = expectLiteralString(bx.Y)
				if err != nil {
					return "", existingMirror{}, err
				}
			}
			if kwarg == "urls" {
				url, err = expectSingletonStringList(bx.Y)
				if err != nil {
					return "", existingMirror{}, err
				}
			}
		default:
			return "", existingMirror{}, fmt.Errorf("unexpected expression in DEPS.bzl: %v", bx)
		}
	}
	if url != "" {
		return name, existingMirror{url: url, sha256: sha256}, nil
	}
	return "", existingMirror{}, nil
}

func expectIdent(x syntax.Expr) (string, error) {
	switch i := x.(type) {
	case *syntax.Ident:
		return i.Name, nil
	default:
		return "", fmt.Errorf("expected identifier, got %v of type %T", i, i)
	}
}

func expectLiteralString(x syntax.Expr) (string, error) {
	switch l := x.(type) {
	case *syntax.Literal:
		switch s := l.Value.(type) {
		case string:
			return s, nil
		default:
			return "", fmt.Errorf("expected literal string, got %v of type %T", s, s)
		}
	default:
		return "", fmt.Errorf("expected literal string, got %v of type %T", l, l)
	}
}

func expectSingletonStringList(x syntax.Expr) (string, error) {
	switch l := x.(type) {
	case *syntax.ListExpr:
		if len(l.List) != 1 {
			return "", fmt.Errorf("expected list to have one item, got %d in %v", len(l.List), l)
		}
		return expectLiteralString(l.List[0])
	default:
		return "", fmt.Errorf("expected list of strings, got %v of type %T", l, l)
	}
}

func mungeBazelRepoNameComponent(component string) string {
	component = strings.ReplaceAll(component, "-", "_")
	component = strings.ReplaceAll(component, ".", "_")
	return strings.ToLower(component)
}

func modulePathToBazelRepoName(mod string) string {
	components := strings.Split(mod, "/")
	head := strings.Split(components[0], ".")
	for i, j := 0, len(head)-1; i < j; i, j = i+1, j-1 {
		head[i], head[j] = mungeBazelRepoNameComponent(head[j]), mungeBazelRepoNameComponent(head[i])
	}
	for index, component := range components {
		if index == 0 {
			continue
		}
		components[index] = mungeBazelRepoNameComponent(component)
	}
	return strings.Join(append(head, components[1:]...), "_")
}

func dumpPatchArgsForRepo(repoName string) error {
	runfiles, err := bazel.RunfilesPath()
	if err != nil {
		return err
	}
	candidate := filepath.Join(runfiles, "build", "patches", repoName+".patch")
	if _, err := os.Stat(candidate); err == nil {
		fmt.Printf(`        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:%s.patch",
        ],
`, repoName)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func buildFileProtoModeForRepo(repoName string) string {
	if repoName == "com_github_prometheus_client_model" {
		return "package"
	}
	return "disable_global"
}

func dumpBuildNamingConventionArgsForRepo(repoName string) {
	if repoName == "com_github_envoyproxy_protoc_gen_validate" || repoName == "com_github_grpc_ecosystem_grpc_gateway" {
		fmt.Printf("        build_naming_convention = \"go_default_library\",\n")
	}
}

func dumpNewDepsBzl(
	listed map[string]listedModule,
	downloaded map[string]downloadedModule,
	existingMirrors map[string]existingMirror,
) error {
	var sorted []string
	repoNameToModPath := make(map[string]string)
	for _, mod := range listed {
		repoName := modulePathToBazelRepoName(mod.Path)
		sorted = append(sorted, repoName)
		repoNameToModPath[repoName] = mod.Path
	}
	sort.Strings(sorted)

	ctx := context.Background()
	var client *storage.Client
	if canMirror() {
		var err error
		client, err = storage.NewClient(ctx)
		if err != nil {
			return err
		}
	}
	g, ctx := errgroup.WithContext(ctx)

	fmt.Println(`load("@bazel_gazelle//:deps.bzl", "go_repository")

# PRO-TIP: You can inject temorary changes to any of these dependencies by
# by pointing to an alternate remote to clone from. Delete the ` + "`sha256`" + `,
# ` + "`strip_prefix`, and ` + `urls`" + ` parameters, and add ` + "`vcs = \"git\"`" + ` as well as a
# custom ` + "`remote` and `commit`" + `. For example:
#     go_repository(
#        name = "com_github_cockroachdb_sentry_go",
#        build_file_proto_mode = "disable_global",
#        importpath = "github.com/cockroachdb/sentry-go",
#        vcs = "git",
#        remote = "https://github.com/rickystewart/sentry-go",  # Custom fork.
#        commit = "6c8e10aca9672de108063d4953399bd331b54037",  # Custom commit.
#    )
# The ` + "`remote` " + `can be EITHER a URL, or an absolute local path to a clone, such
# as ` + "`/Users/ricky/go/src/github.com/cockroachdb/sentry-go`" + `. Bazel will clone
# from the remote and check out the commit you specify.

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)`)
	for _, repoName := range sorted {
		path := repoNameToModPath[repoName]
		mod := listed[path]
		replaced := &mod
		if mod.Replace != nil {
			replaced = mod.Replace
		}
		fmt.Printf(`    go_repository(
        name = "%s",
        build_file_proto_mode = "%s",
`, repoName, buildFileProtoModeForRepo(repoName))

		dumpBuildNamingConventionArgsForRepo(repoName)
		expectedURL := formatURL(replaced.Path, replaced.Version)
		fmt.Printf("        importpath = \"%s\",\n", mod.Path)
		if err := dumpPatchArgsForRepo(repoName); err != nil {
			return err
		}
		oldMirror, ok := existingMirrors[repoName]
		if ok && oldMirror.url == expectedURL {
			// The URL matches, so just reuse the old mirror.
			fmt.Printf(`        sha256 = "%s",
        strip_prefix = "%s@%s",
        urls = [
            "%s",
        ],
`, oldMirror.sha256, replaced.Path, replaced.Version, oldMirror.url)
		} else if canMirror() {
			// We'll have to mirror our copy of the zip ourselves.
			d := downloaded[replaced.Path]
			sha, err := getSha256OfFile(d.Zip)
			if err != nil {
				return fmt.Errorf("could not get zip for %v: %w", *replaced, err)
			}
			fmt.Printf(`        sha256 = "%s",
        strip_prefix = "%s@%s",
        urls = [
            "%s",
        ],
`, sha, replaced.Path, replaced.Version, expectedURL)
			g.Go(func() error {
				return uploadFile(ctx, client, d.Zip, formatSubURL(replaced.Path, replaced.Version))
			})
		} else {
			// We don't have a mirror and can't upload one, so just
			// have Gazelle pull the repo for us.
			d := downloaded[replaced.Path]
			if mod.Replace != nil {
				fmt.Printf("        replace = \"%s\",\n", replaced.Path)
			}
			// Note: `build/teamcity-check-genfiles.sh` checks for
			// the presence of the "TODO: mirror this repo" comment.
			// Don't update this comment without also updating the
			// script.
			fmt.Printf(`        # TODO: mirror this repo (to fix, run `+"`./dev generate bazel --mirror`)"+`
        sum = "%s",
        version = "%s",
`, d.Sum, d.Version)
		}
		fmt.Println("    )")
	}

	// Wait for uploads to complete.
	if err := g.Wait(); err != nil {
		return err
	}
	if client == nil {
		return nil
	}
	return client.Close()
}

func mirror() error {
	tmpdir, err := createTmpDir()
	if err != nil {
		return err
	}
	defer func() {
		err := os.RemoveAll(tmpdir)
		if err != nil {
			panic(err)
		}
	}()
	downloaded, err := downloadZips(tmpdir)
	if err != nil {
		return err
	}
	listed, err := listAllModules(tmpdir)
	if err != nil {
		return err
	}
	existingMirrors, err := getExistingMirrors()
	if err != nil {
		return err
	}

	return dumpNewDepsBzl(listed, downloaded, existingMirrors)
}

func main() {
	if err := mirror(); err != nil {
		panic(err)
	}
}
