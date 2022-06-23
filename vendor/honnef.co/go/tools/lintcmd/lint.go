package lintcmd

import (
	"crypto/sha256"
	"fmt"
	"go/build"
	"go/token"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"honnef.co/go/tools/analysis/lint"
	"honnef.co/go/tools/config"
	"honnef.co/go/tools/go/buildid"
	"honnef.co/go/tools/go/loader"
	"honnef.co/go/tools/lintcmd/cache"
	"honnef.co/go/tools/lintcmd/runner"
	"honnef.co/go/tools/unused"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/packages"
)

// A linter lints Go source code.
type linter struct {
	Analyzers map[string]*lint.Analyzer
	Runner    *runner.Runner
}

func computeSalt() ([]byte, error) {
	p, err := os.Executable()
	if err != nil {
		return nil, err
	}

	if id, err := buildid.ReadFile(p); err == nil {
		return []byte(id), nil
	} else {
		// For some reason we couldn't read the build id from the executable.
		// Fall back to hashing the entire executable.
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
}

func newLinter(cfg config.Config) (*linter, error) {
	c, err := cache.Default()
	if err != nil {
		return nil, err
	}
	salt, err := computeSalt()
	if err != nil {
		return nil, fmt.Errorf("could not compute salt for cache: %s", err)
	}
	c.SetSalt(salt)
	r, err := runner.New(cfg, c)
	if err != nil {
		return nil, err
	}
	r.FallbackGoVersion = defaultGoVersion()
	return &linter{
		Runner: r,
	}, nil
}

type LintResult struct {
	CheckedFiles []string
	Diagnostics  []diagnostic
	Warnings     []string
}

func (l *linter) Lint(cfg *packages.Config, patterns []string) (LintResult, error) {
	var out LintResult

	as := make([]*analysis.Analyzer, 0, len(l.Analyzers))
	for _, a := range l.Analyzers {
		as = append(as, a.Analyzer)
	}
	results, err := l.Runner.Run(cfg, as, patterns)
	if err != nil {
		return out, err
	}

	if len(results) == 0 {
		// TODO(dh): emulate Go's behavior more closely once we have
		// access to go list's Match field.
		for _, pattern := range patterns {
			fmt.Fprintf(os.Stderr, "warning: %q matched no packages\n", pattern)
		}
	}

	analyzerNames := make([]string, 0, len(l.Analyzers))
	for name := range l.Analyzers {
		analyzerNames = append(analyzerNames, name)
	}
	used := map[unusedKey]bool{}
	var unuseds []unusedPair
	for _, res := range results {
		if len(res.Errors) > 0 && !res.Failed {
			panic("package has errors but isn't marked as failed")
		}
		if res.Failed {
			out.Diagnostics = append(out.Diagnostics, failed(res)...)
		} else {
			if res.Skipped {
				out.Warnings = append(out.Warnings, fmt.Sprintf("skipped package %s because it is too large", res.Package))
				continue
			}

			if !res.Initial {
				continue
			}

			out.CheckedFiles = append(out.CheckedFiles, res.Package.GoFiles...)
			allowedAnalyzers := filterAnalyzerNames(analyzerNames, res.Config.Checks)
			resd, err := res.Load()
			if err != nil {
				return out, err
			}
			ps := success(allowedAnalyzers, resd)
			filtered, err := filterIgnored(ps, resd, allowedAnalyzers)
			if err != nil {
				return out, err
			}
			// OPT move this code into the 'success' function.
			for i, diag := range filtered {
				a := l.Analyzers[diag.Category]
				// Some diag.Category don't map to analyzers, such as "staticcheck"
				if a != nil {
					filtered[i].MergeIf = a.Doc.MergeIf
				}
			}
			out.Diagnostics = append(out.Diagnostics, filtered...)

			for _, obj := range resd.Unused.Used {
				// FIXME(dh): pick the object whose filename does not include $GOROOT
				key := unusedKey{
					pkgPath: res.Package.PkgPath,
					base:    filepath.Base(obj.Position.Filename),
					line:    obj.Position.Line,
					name:    obj.Name,
				}
				used[key] = true
			}

			if allowedAnalyzers["U1000"] {
				for _, obj := range resd.Unused.Unused {
					key := unusedKey{
						pkgPath: res.Package.PkgPath,
						base:    filepath.Base(obj.Position.Filename),
						line:    obj.Position.Line,
						name:    obj.Name,
					}
					unuseds = append(unuseds, unusedPair{key, obj})
					if _, ok := used[key]; !ok {
						used[key] = false
					}
				}
			}
		}
	}

	for _, uo := range unuseds {
		if uo.obj.Kind == "type param" {
			// We don't currently flag unused type parameters on used objects, and flagging them on unused objects isn't
			// useful.
			continue
		}
		if used[uo.key] {
			continue
		}
		if uo.obj.InGenerated {
			continue
		}
		out.Diagnostics = append(out.Diagnostics, diagnostic{
			Diagnostic: runner.Diagnostic{
				Position: uo.obj.DisplayPosition,
				Message:  fmt.Sprintf("%s %s is unused", uo.obj.Kind, uo.obj.Name),
				Category: "U1000",
			},
			MergeIf: lint.MergeIfAll,
		})
	}

	return out, nil
}

func filterIgnored(diagnostics []diagnostic, res runner.ResultData, allowedAnalyzers map[string]bool) ([]diagnostic, error) {
	couldHaveMatched := func(ig *lineIgnore) bool {
		for _, c := range ig.Checks {
			if c == "U1000" {
				// We never want to flag ignores for U1000,
				// because U1000 isn't local to a single
				// package. For example, an identifier may
				// only be used by tests, in which case an
				// ignore would only fire when not analyzing
				// tests. To avoid spurious "useless ignore"
				// warnings, just never flag U1000.
				return false
			}

			// Even though the runner always runs all analyzers, we
			// still only flag unmatched ignores for the set of
			// analyzers the user has expressed interest in. That way,
			// `staticcheck -checks=SA1000` won't complain about an
			// unmatched ignore for an unrelated check.
			if allowedAnalyzers[c] {
				return true
			}
		}

		return false
	}

	ignores, moreDiagnostics := parseDirectives(res.Directives)

	for _, ig := range ignores {
		for i := range diagnostics {
			diag := &diagnostics[i]
			if ig.Match(*diag) {
				diag.Severity = severityIgnored
			}
		}

		if ig, ok := ig.(*lineIgnore); ok && !ig.Matched && couldHaveMatched(ig) {
			diag := diagnostic{
				Diagnostic: runner.Diagnostic{
					Position: ig.Pos,
					Message:  "this linter directive didn't match anything; should it be removed?",
					Category: "staticcheck",
				},
			}
			moreDiagnostics = append(moreDiagnostics, diag)
		}
	}

	return append(diagnostics, moreDiagnostics...), nil
}

type ignore interface {
	Match(diag diagnostic) bool
}

type lineIgnore struct {
	File    string
	Line    int
	Checks  []string
	Matched bool
	Pos     token.Position
}

func (li *lineIgnore) Match(p diagnostic) bool {
	pos := p.Position
	if pos.Filename != li.File || pos.Line != li.Line {
		return false
	}
	for _, c := range li.Checks {
		if m, _ := filepath.Match(c, p.Category); m {
			li.Matched = true
			return true
		}
	}
	return false
}

func (li *lineIgnore) String() string {
	matched := "not matched"
	if li.Matched {
		matched = "matched"
	}
	return fmt.Sprintf("%s:%d %s (%s)", li.File, li.Line, strings.Join(li.Checks, ", "), matched)
}

type fileIgnore struct {
	File   string
	Checks []string
}

func (fi *fileIgnore) Match(p diagnostic) bool {
	if p.Position.Filename != fi.File {
		return false
	}
	for _, c := range fi.Checks {
		if m, _ := filepath.Match(c, p.Category); m {
			return true
		}
	}
	return false
}

type severity uint8

const (
	severityError severity = iota
	severityWarning
	severityIgnored
)

func (s severity) String() string {
	switch s {
	case severityError:
		return "error"
	case severityWarning:
		return "warning"
	case severityIgnored:
		return "ignored"
	default:
		return fmt.Sprintf("Severity(%d)", s)
	}
}

// diagnostic represents a diagnostic in some source code.
type diagnostic struct {
	runner.Diagnostic
	Severity  severity
	MergeIf   lint.MergeStrategy
	BuildName string
}

func (p diagnostic) equal(o diagnostic) bool {
	return p.Position == o.Position &&
		p.End == o.End &&
		p.Message == o.Message &&
		p.Category == o.Category &&
		p.Severity == o.Severity &&
		p.MergeIf == o.MergeIf &&
		p.BuildName == o.BuildName
}

func (p *diagnostic) String() string {
	if p.BuildName != "" {
		return fmt.Sprintf("%s [%s] (%s)", p.Message, p.BuildName, p.Category)
	} else {
		return fmt.Sprintf("%s (%s)", p.Message, p.Category)
	}
}

func failed(res runner.Result) []diagnostic {
	var diagnostics []diagnostic

	for _, e := range res.Errors {
		switch e := e.(type) {
		case packages.Error:
			msg := e.Msg
			if len(msg) != 0 && msg[0] == '\n' {
				// TODO(dh): See https://github.com/golang/go/issues/32363
				msg = msg[1:]
			}

			var posn token.Position
			if e.Pos == "" {
				// Under certain conditions (malformed package
				// declarations, multiple packages in the same
				// directory), go list emits an error on stderr
				// instead of JSON. Those errors do not have
				// associated position information in
				// go/packages.Error, even though the output on
				// stderr may contain it.
				if p, n, err := parsePos(msg); err == nil {
					if abs, err := filepath.Abs(p.Filename); err == nil {
						p.Filename = abs
					}
					posn = p
					msg = msg[n+2:]
				}
			} else {
				var err error
				posn, _, err = parsePos(e.Pos)
				if err != nil {
					panic(fmt.Sprintf("internal error: %s", e))
				}
			}
			diag := diagnostic{
				Diagnostic: runner.Diagnostic{
					Position: posn,
					Message:  msg,
					Category: "compile",
				},
				Severity: severityError,
			}
			diagnostics = append(diagnostics, diag)
		case error:
			diag := diagnostic{
				Diagnostic: runner.Diagnostic{
					Position: token.Position{},
					Message:  e.Error(),
					Category: "compile",
				},
				Severity: severityError,
			}
			diagnostics = append(diagnostics, diag)
		}
	}

	return diagnostics
}

type unusedKey struct {
	pkgPath string
	base    string
	line    int
	name    string
}

type unusedPair struct {
	key unusedKey
	obj unused.SerializedObject
}

func success(allowedAnalyzers map[string]bool, res runner.ResultData) []diagnostic {
	diags := res.Diagnostics
	var diagnostics []diagnostic
	for _, diag := range diags {
		if !allowedAnalyzers[diag.Category] {
			continue
		}
		diagnostics = append(diagnostics, diagnostic{Diagnostic: diag})
	}
	return diagnostics
}

func defaultGoVersion() string {
	tags := build.Default.ReleaseTags
	v := tags[len(tags)-1][2:]
	return v
}

func filterAnalyzerNames(analyzers []string, checks []string) map[string]bool {
	allowedChecks := map[string]bool{}

	for _, check := range checks {
		b := true
		if len(check) > 1 && check[0] == '-' {
			b = false
			check = check[1:]
		}
		if check == "*" || check == "all" {
			// Match all
			for _, c := range analyzers {
				allowedChecks[c] = b
			}
		} else if strings.HasSuffix(check, "*") {
			// Glob
			prefix := check[:len(check)-1]
			isCat := strings.IndexFunc(prefix, func(r rune) bool { return unicode.IsNumber(r) }) == -1

			for _, a := range analyzers {
				idx := strings.IndexFunc(a, func(r rune) bool { return unicode.IsNumber(r) })
				if isCat {
					// Glob is S*, which should match S1000 but not SA1000
					cat := a[:idx]
					if prefix == cat {
						allowedChecks[a] = b
					}
				} else {
					// Glob is S1*
					if strings.HasPrefix(a, prefix) {
						allowedChecks[a] = b
					}
				}
			}
		} else {
			// Literal check name
			allowedChecks[check] = b
		}
	}
	return allowedChecks
}

var posRe = regexp.MustCompile(`^(.+?):(\d+)(?::(\d+)?)?`)

func parsePos(pos string) (token.Position, int, error) {
	if pos == "-" || pos == "" {
		return token.Position{}, 0, nil
	}
	parts := posRe.FindStringSubmatch(pos)
	if parts == nil {
		return token.Position{}, 0, fmt.Errorf("internal error: malformed position %q", pos)
	}
	file := parts[1]
	line, _ := strconv.Atoi(parts[2])
	col, _ := strconv.Atoi(parts[3])
	return token.Position{
		Filename: file,
		Line:     line,
		Column:   col,
	}, len(parts[0]), nil
}

type options struct {
	Config      config.Config
	BuildConfig BuildConfig

	LintTests                bool
	GoVersion                string
	PrintAnalyzerMeasurement func(analysis *analysis.Analyzer, pkg *loader.PackageSpec, d time.Duration)
}

func doLint(as []*lint.Analyzer, paths []string, opt *options) (LintResult, error) {
	if opt == nil {
		opt = &options{}
	}

	l, err := newLinter(opt.Config)
	if err != nil {
		return LintResult{}, err
	}
	analyzers := make(map[string]*lint.Analyzer, len(as))
	for _, a := range as {
		analyzers[a.Analyzer.Name] = a
	}
	l.Analyzers = analyzers
	l.Runner.GoVersion = opt.GoVersion
	l.Runner.Stats.PrintAnalyzerMeasurement = opt.PrintAnalyzerMeasurement

	cfg := &packages.Config{}
	if opt.LintTests {
		cfg.Tests = true
	}

	cfg.BuildFlags = opt.BuildConfig.Flags
	cfg.Env = append(os.Environ(), opt.BuildConfig.Envs...)

	printStats := func() {
		// Individual stats are read atomically, but overall there
		// is no synchronisation. For printing rough progress
		// information, this doesn't matter.
		switch l.Runner.Stats.State() {
		case runner.StateInitializing:
			fmt.Fprintln(os.Stderr, "Status: initializing")
		case runner.StateLoadPackageGraph:
			fmt.Fprintln(os.Stderr, "Status: loading package graph")
		case runner.StateBuildActionGraph:
			fmt.Fprintln(os.Stderr, "Status: building action graph")
		case runner.StateProcessing:
			fmt.Fprintf(os.Stderr, "Packages: %d/%d initial, %d/%d total; Workers: %d/%d\n",
				l.Runner.Stats.ProcessedInitialPackages(),
				l.Runner.Stats.InitialPackages(),
				l.Runner.Stats.ProcessedPackages(),
				l.Runner.Stats.TotalPackages(),
				l.Runner.ActiveWorkers(),
				l.Runner.TotalWorkers(),
			)
		case runner.StateFinalizing:
			fmt.Fprintln(os.Stderr, "Status: finalizing")
		}
	}
	if len(infoSignals) > 0 {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, infoSignals...)
		defer signal.Stop(ch)
		go func() {
			for range ch {
				printStats()
			}
		}()
	}
	res, err := l.Lint(cfg, paths)
	for i := range res.Diagnostics {
		res.Diagnostics[i].BuildName = opt.BuildConfig.Name
	}
	return res, err
}
