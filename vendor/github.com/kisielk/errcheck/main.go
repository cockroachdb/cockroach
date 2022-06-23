package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/kisielk/errcheck/errcheck"
	"golang.org/x/tools/go/packages"
)

const (
	exitCodeOk int = iota
	exitUncheckedError
	exitFatalError
)

type ignoreFlag map[string]*regexp.Regexp

// global flags
var (
	abspath bool
	verbose bool
)

func (f ignoreFlag) String() string {
	pairs := make([]string, 0, len(f))
	for pkg, re := range f {
		prefix := ""
		if pkg != "" {
			prefix = pkg + ":"
		}
		pairs = append(pairs, prefix+re.String())
	}
	return fmt.Sprintf("%q", strings.Join(pairs, ","))
}

func (f ignoreFlag) Set(s string) error {
	if s == "" {
		return nil
	}
	for _, pair := range strings.Split(s, ",") {
		colonIndex := strings.Index(pair, ":")
		var pkg, re string
		if colonIndex == -1 {
			pkg = ""
			re = pair
		} else {
			pkg = pair[:colonIndex]
			re = pair[colonIndex+1:]
		}
		regex, err := regexp.Compile(re)
		if err != nil {
			return err
		}
		f[pkg] = regex
	}
	return nil
}

type tagsFlag []string

func (f *tagsFlag) String() string {
	return fmt.Sprintf("%q", strings.Join(*f, ","))
}

func (f *tagsFlag) Set(s string) error {
	if s == "" {
		return nil
	}
	tags := strings.FieldsFunc(s, func(c rune) bool {
		return c == ' ' || c == ','
	})
	for _, tag := range tags {
		if tag != "" {
			*f = append(*f, tag)
		}
	}
	return nil
}

func reportResult(e errcheck.Result) {
	wd, err := os.Getwd()
	if err != nil {
		wd = ""
	}
	for _, uncheckedError := range e.UncheckedErrors {
		pos := uncheckedError.Pos.String()
		if !abspath {
			newPos, err := filepath.Rel(wd, pos)
			if err == nil {
				pos = newPos
			}
		}

		if verbose && uncheckedError.FuncName != "" {
			fmt.Printf("%s:\t%s\t%s\n", pos, uncheckedError.FuncName, uncheckedError.Line)
		} else {
			fmt.Printf("%s:\t%s\n", pos, uncheckedError.Line)
		}
	}
}

func logf(msg string, args ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, msg+"\n", args...)
	}
}

func mainCmd(args []string) int {
	var checker errcheck.Checker
	paths, rc := parseFlags(&checker, args)
	if rc != exitCodeOk {
		return rc
	}

	result, err := checkPaths(&checker, paths...)
	if err != nil {
		if err == errcheck.ErrNoGoFiles {
			fmt.Fprintln(os.Stderr, err)
			return exitCodeOk
		}
		fmt.Fprintf(os.Stderr, "error: failed to check packages: %s\n", err)
		return exitFatalError
	}
	if len(result.UncheckedErrors) > 0 {
		reportResult(result)
		return exitUncheckedError
	}
	return exitCodeOk
}

func checkPaths(c *errcheck.Checker, paths ...string) (errcheck.Result, error) {
	pkgs, err := c.LoadPackages(paths...)
	if err != nil {
		return errcheck.Result{}, err
	}
	// Check for errors in the initial packages.
	work := make(chan *packages.Package, len(pkgs))
	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			return errcheck.Result{}, fmt.Errorf("errors while loading package %s: %v", pkg.ID, pkg.Errors)
		}
		work <- pkg
	}
	close(work)

	var wg sync.WaitGroup
	result := &errcheck.Result{}
	mu := &sync.Mutex{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for pkg := range work {
				logf("checking %s", pkg.Types.Path())
				r := c.CheckPackage(pkg)
				mu.Lock()
				result.Append(r)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return result.Unique(), nil
}

func parseFlags(checker *errcheck.Checker, args []string) ([]string, int) {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)

	var checkAsserts, checkBlanks bool

	flags.BoolVar(&checkBlanks, "blank", false, "if true, check for errors assigned to blank identifier")
	flags.BoolVar(&checkAsserts, "asserts", false, "if true, check for ignored type assertion results")
	flags.BoolVar(&checker.Exclusions.TestFiles, "ignoretests", false, "if true, checking of _test.go files is disabled")
	flags.BoolVar(&checker.Exclusions.GeneratedFiles, "ignoregenerated", false, "if true, checking of files with generated code is disabled")
	flags.BoolVar(&verbose, "verbose", false, "produce more verbose logging")

	flags.BoolVar(&abspath, "abspath", false, "print absolute paths to files")

	tags := tagsFlag{}
	flags.Var(&tags, "tags", "comma or space-separated list of build tags to include")
	ignorePkg := flags.String("ignorepkg", "", "comma-separated list of package paths to ignore")
	ignore := ignoreFlag(map[string]*regexp.Regexp{})
	flags.Var(ignore, "ignore", "[deprecated] comma-separated list of pairs of the form pkg:regex\n"+
		"            the regex is used to ignore names within pkg.")

	var excludeFile string
	flags.StringVar(&excludeFile, "exclude", "", "Path to a file containing a list of functions to exclude from checking")

	var excludeOnly bool
	flags.BoolVar(&excludeOnly, "excludeonly", false, "Use only excludes from -exclude file")

	flags.StringVar(&checker.Mod, "mod", "", "module download mode to use: readonly or vendor. See 'go help modules' for more.")

	if err := flags.Parse(args[1:]); err != nil {
		return nil, exitFatalError
	}

	checker.Exclusions.BlankAssignments = !checkBlanks
	checker.Exclusions.TypeAssertions = !checkAsserts

	if !excludeOnly {
		checker.Exclusions.Symbols = append(checker.Exclusions.Symbols, errcheck.DefaultExcludedSymbols...)
	}

	if excludeFile != "" {
		excludes, err := errcheck.ReadExcludes(excludeFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not read exclude file: %v\n", err)
			return nil, exitFatalError
		}
		checker.Exclusions.Symbols = append(checker.Exclusions.Symbols, excludes...)
	}

	checker.Tags = tags
	for _, pkg := range strings.Split(*ignorePkg, ",") {
		if pkg != "" {
			checker.Exclusions.Packages = append(checker.Exclusions.Packages, pkg)
		}
	}

	checker.Exclusions.SymbolRegexpsByPackage = ignore

	paths := flags.Args()
	if len(paths) == 0 {
		paths = []string{"."}
	}

	return paths, exitCodeOk
}

func main() {
	os.Exit(mainCmd(os.Args))
}
