package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go/build"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
)

var cmdSave = &Command{
	UsageLine: "save [import path]",
	Short:     "save a GLOCKFILE for the given package's dependencies",
	Long: `save is used to record the current revisions of a package's dependencies

It writes this state to a file in the root of the package called "GLOCKFILE".

Options:

	-n	print to stdout instead of writing to file.

`,
}

var saveN = cmdSave.Flag.Bool("n", false, "Don't save the file, just print to stdout")

func init() {
	cmdSave.Run = runSave // break init loop
}

func runSave(cmd *Command, args []string) {
	if len(args) == 0 {
		cmdSave.Usage()
		return
	}

	// Read cmd lines from GLOCKFILE and calculate required dependencies.
	var (
		importPath = args[0]
		cmds       = readCmds(importPath)
		depRoots   = calcDepRoots(importPath, cmds)
	)

	output := glockfileWriter(importPath, *saveN)
	outputCmds(output, cmds)
	outputDeps(output, depRoots)
	output.Close()
}

func outputCmds(w io.Writer, cmds []string) {
	sort.Strings(cmds)
	var prev string
	for _, cmd := range cmds {
		if cmd != prev {
			fmt.Fprintln(w, "cmd", cmd)
		}
		prev = cmd
	}
}

func outputDeps(w io.Writer, depRoots []*repoRoot) {
	for _, repoRoot := range depRoots {
		revision, err := repoRoot.vcs.head(
			path.Join(gopath(), "src", repoRoot.root),
			repoRoot.repo)
		if err != nil {
			perror(err)
		}
		revision = strings.TrimSpace(revision)
		fmt.Fprintln(w, repoRoot.root, revision)
	}
}

// calcDepRoots discovers all dependencies of the given importPath and returns
// them as a list of the repo roots that cover all dependent packages. for
// example, github.com/robfig/soy and github.com/robfig/soy/data are two
// dependent packages but only one repo. the returned repos are ordered
// alphabetically by import path.
func calcDepRoots(importPath string, cmds []string) []*repoRoot {
	var attempts = 1
GetAllDeps:
	var depRoots = map[string]*repoRoot{}
	var missingPackages []string
	for _, importPath := range getAllDeps(importPath, cmds) {
		// Convert from packages to repo roots.
		// TODO: Filter out any packages that have prefixes also included in the list.
		// e.g. pkg/foo/bar , pkg/foo/baz , pkg/foo
		// That would skip the relatively slow (redundant) determining of repo root for each.
		var repoRoot, err = glockRepoRootForImportPath(importPath)
		if err != nil {
			perror(err)
		}

		// Ensure we have the package locally. if not, we don't have all the possible deps.
		_, err = build.Import(repoRoot.root, "", build.FindOnly)
		if err != nil {
			missingPackages = append(missingPackages, repoRoot.root)
		}

		depRoots[repoRoot.root] = repoRoot
	}

	// If there were missing packages, try again.
	if len(missingPackages) > 0 {
		if attempts > 3 {
			perror(fmt.Errorf("failed to fetch missing packages: %v", missingPackages))
		}
		fmt.Fprintln(os.Stderr, "go", "get", "-d", strings.Join(missingPackages, " "))
		run("go", append([]string{"get", "-d"}, missingPackages...)...)
		attempts++
		goto GetAllDeps
	}

	// Remove any dependencies to packages within the target repo
	delete(depRoots, importPath)

	var repos []*repoRoot
	for _, repo := range depRoots {
		repos = append(repos, repo)
	}
	sort.Sort(byImportPath(repos))
	return repos
}

type byImportPath []*repoRoot

func (p byImportPath) Len() int           { return len(p) }
func (p byImportPath) Less(i, j int) bool { return p[i].root < p[j].root }
func (p byImportPath) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// getAllDeps returns a slice of package import paths for all dependencies
// (including test dependencies) of the given import path (and subpackages) and commands.
func getAllDeps(importPath string, cmds []string) []string {
	// Get a set of transitive dependencies (package import paths) for the
	// specified package.
	var pkgExprs = append([]string{path.Join(importPath, "...")}, cmds...)
	var output = mustRun("go",
		append([]string{"list", "-f", `{{range .Deps}}{{.}}{{"\n"}}{{end}}`}, pkgExprs...)...)
	var deps = filterPackages(output, nil) // filter out standard library

	// Add the command packages.
	for _, cmd := range cmds {
		deps[cmd] = struct{}{}
	}

	// List dependencies of test files, which are not included in the go list .Deps
	// Also, ignore any dependencies that are already covered.
	var testImportOutput = mustRun("go",
		append([]string{"list", "-f", `{{join .TestImports "\n"}}{{"\n"}}{{join .XTestImports "\n"}}`}, pkgExprs...)...)
	var testImmediateDeps = filterPackages(testImportOutput, deps) // filter out stdlib and existing deps
	for dep := range testImmediateDeps {
		deps[dep] = struct{}{}
	}

	// We have to get the transitive deps of the remaining test imports.
	// NOTE: this will return the dependencies of the libraries imported by tests
	// and not imported by main code.  This output does not include the imports themselves.
	if len(testImmediateDeps) > 0 {
		var testDepOutput = mustRun("go", append([]string{"list", "-f", `{{range .Deps}}{{.}}{{"\n"}}{{end}}`}, setToSlice(testImmediateDeps)...)...)
		var allTestDeps = filterPackages(testDepOutput, deps) // filter out standard library and existing deps
		for dep := range allTestDeps {
			deps[dep] = struct{}{}
		}
	}

	// Return everything in deps
	var result []string
	for dep := range deps {
		result = append(result, dep)
	}
	return result
}

func run(name string, args ...string) ([]byte, error) {
	if buildV {
		fmt.Println(name, args)
	}
	var cmd = exec.Command(name, args...)
	return cmd.CombinedOutput()
}

// mustRun is a wrapper for exec.Command(..).CombinedOutput() that provides helpful
// error message and exits on failure.
func mustRun(name string, args ...string) []byte {
	var output, err = run(name, args...)
	if err != nil {
		perror(fmt.Errorf("%v %v\n%v\nError: %v", name, args, string(output), err))
	}
	return output
}

func setToSlice(set map[string]struct{}) []string {
	var slice []string
	for k := range set {
		slice = append(slice, k)
	}
	return slice
}

// filterPackages accepts the output of a go list comment (one package per line)
// and returns a set of package import paths, excluding standard library.
// Additionally, any packages present in the "exclude" set will be excluded.
func filterPackages(output []byte, exclude map[string]struct{}) map[string]struct{} {
	var scanner = bufio.NewScanner(bytes.NewReader(output))
	var deps = map[string]struct{}{}
	for scanner.Scan() {
		var (
			pkg    = scanner.Text()
			slash  = strings.Index(pkg, "/")
			stdLib = slash == -1 || strings.Index(pkg[:slash], ".") == -1
		)
		if stdLib {
			continue
		}
		if _, ok := exclude[pkg]; ok {
			continue
		}
		deps[pkg] = struct{}{}
	}
	return deps
}

// readCmds returns the list of cmds declared in the given glockfile.
// They must appear at the top of the file, with the syntax:
//   cmd code.google.com/p/go.tools/cmd/godoc
//   cmd github.com/robfig/glock
func readCmds(importPath string) []string {
	var glockfile, err = os.Open(glockFilename(importPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		perror(err)
	}

	var cmds []string
	var scanner = bufio.NewScanner(glockfile)
	for scanner.Scan() {
		var fields = strings.Fields(scanner.Text())
		if len(fields) != 2 || fields[0] != "cmd" {
			return cmds
		}
		cmds = append(cmds, fields[1])
	}
	if err := scanner.Err(); err != nil {
		perror(err)
	}
	return cmds
}

// Keep edits to vcs.go separate from the stock version.

var headCmds = map[string]string{
	"git": "rev-parse HEAD",  // 2bebebd91805dbb931317f7a4057e4e8de9d9781
	"hg":  "id",              // 19114a3ee7d5+ tip
	"bzr": "log -r-1 --line", // 50: Dimiter Naydenov 2014-02-12 [merge] ec2: Added (Un)AssignPrivateIPAddresses APIs
}

var (
	revisionSeparator = regexp.MustCompile(`[ :+]+`)
	validRevision     = regexp.MustCompile(`^[\d\w]+$`)
)

func (v *vcsCmd) head(dir, repo string) (string, error) {
	var output, err = v.runOutput(dir, headCmds[v.cmd], "dir", dir, "repo", repo)
	if err != nil {
		return "", err
	}
	return parseHEAD(output)
}

func parseHEAD(output []byte) (string, error) {
	// Handle a case where HG returns success but prints an error, causing our
	// parsing of the revision id to break.
	var str = strings.TrimSpace(string(output))
	for strings.HasPrefix(str, "*** ") {
		var i = strings.Index(str, "\n")
		if i == -1 {
			break
		}
		str = str[i+1:]
	}

	var head = revisionSeparator.Split(str, -1)[0]
	if !validRevision.MatchString(head) {
		fmt.Fprintln(os.Stderr, string(output))
		return "", errors.New("error getting head revision")
	}
	return head, nil
}
