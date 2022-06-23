// Package lint provides abstractions on top of go/analysis.
// These abstractions add extra information to analyzes, such as structured documentation and severities.
package lint

import (
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/token"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer wraps a go/analysis.Analyzer and provides structured documentation.
type Analyzer struct {
	// The analyzer's documentation. Unlike go/analysis.Analyzer.Doc,
	// this field is structured, providing access to severity, options
	// etc.
	Doc      *Documentation
	Analyzer *analysis.Analyzer
}

func (a *Analyzer) initialize() {
	a.Analyzer.Doc = a.Doc.String()
	if a.Analyzer.Flags.Usage == nil {
		fs := flag.NewFlagSet("", flag.PanicOnError)
		fs.Var(newVersionFlag(), "go", "Target Go version")
		a.Analyzer.Flags = *fs
	}
}

// InitializeAnalyzers takes a map of documentation and a map of go/analysis.Analyzers and returns a slice of Analyzers.
// The map keys are the analyzer names.
func InitializeAnalyzers(docs map[string]*Documentation, analyzers map[string]*analysis.Analyzer) []*Analyzer {
	out := make([]*Analyzer, 0, len(analyzers))
	for k, v := range analyzers {
		v.Name = k
		a := &Analyzer{
			Doc:      docs[k],
			Analyzer: v,
		}
		a.initialize()
		out = append(out, a)
	}
	return out
}

// Severity describes the severity of diagnostics reported by an analyzer.
type Severity int

const (
	SeverityNone Severity = iota
	SeverityError
	SeverityDeprecated
	SeverityWarning
	SeverityInfo
	SeverityHint
)

// MergeStrategy sets how merge mode should behave for diagnostics of an analyzer.
type MergeStrategy int

const (
	MergeIfAny MergeStrategy = iota
	MergeIfAll
)

type RawDocumentation struct {
	Title      string
	Text       string
	Before     string
	After      string
	Since      string
	NonDefault bool
	Options    []string
	Severity   Severity
	MergeIf    MergeStrategy
}

type Documentation struct {
	Title string
	Text  string

	TitleMarkdown string
	TextMarkdown  string

	Before     string
	After      string
	Since      string
	NonDefault bool
	Options    []string
	Severity   Severity
	MergeIf    MergeStrategy
}

func Markdownify(m map[string]*RawDocumentation) map[string]*Documentation {
	out := make(map[string]*Documentation, len(m))
	for k, v := range m {
		out[k] = &Documentation{
			Title: strings.TrimSpace(stripMarkdown(v.Title)),
			Text:  strings.TrimSpace(stripMarkdown(v.Text)),

			TitleMarkdown: strings.TrimSpace(toMarkdown(v.Title)),
			TextMarkdown:  strings.TrimSpace(toMarkdown(v.Text)),

			Before:     strings.TrimSpace(v.Before),
			After:      strings.TrimSpace(v.After),
			Since:      v.Since,
			NonDefault: v.NonDefault,
			Options:    v.Options,
			Severity:   v.Severity,
			MergeIf:    v.MergeIf,
		}
	}
	return out
}

func toMarkdown(s string) string {
	return strings.NewReplacer(`\'`, "`", `\"`, "`").Replace(s)
}

func stripMarkdown(s string) string {
	return strings.NewReplacer(`\'`, "", `\"`, "'").Replace(s)
}

func (doc *Documentation) Format(metadata bool) string {
	return doc.format(false, metadata)
}

func (doc *Documentation) FormatMarkdown(metadata bool) string {
	return doc.format(true, metadata)
}

func (doc *Documentation) format(markdown bool, metadata bool) string {
	b := &strings.Builder{}
	if markdown {
		fmt.Fprintf(b, "%s\n\n", doc.TitleMarkdown)
		if doc.Text != "" {
			fmt.Fprintf(b, "%s\n\n", doc.TextMarkdown)
		}
	} else {
		fmt.Fprintf(b, "%s\n\n", doc.Title)
		if doc.Text != "" {
			fmt.Fprintf(b, "%s\n\n", doc.Text)
		}
	}

	if doc.Before != "" {
		fmt.Fprintln(b, "Before:")
		fmt.Fprintln(b, "")
		for _, line := range strings.Split(doc.Before, "\n") {
			fmt.Fprint(b, "    ", line, "\n")
		}
		fmt.Fprintln(b, "")
		fmt.Fprintln(b, "After:")
		fmt.Fprintln(b, "")
		for _, line := range strings.Split(doc.After, "\n") {
			fmt.Fprint(b, "    ", line, "\n")
		}
		fmt.Fprintln(b, "")
	}

	if metadata {
		fmt.Fprint(b, "Available since\n    ")
		if doc.Since == "" {
			fmt.Fprint(b, "unreleased")
		} else {
			fmt.Fprintf(b, "%s", doc.Since)
		}
		if doc.NonDefault {
			fmt.Fprint(b, ", non-default")
		}
		fmt.Fprint(b, "\n")
		if len(doc.Options) > 0 {
			fmt.Fprintf(b, "\nOptions\n")
			for _, opt := range doc.Options {
				fmt.Fprintf(b, "    %s", opt)
			}
			fmt.Fprint(b, "\n")
		}
	}

	return b.String()
}

func (doc *Documentation) String() string {
	return doc.Format(true)
}

func newVersionFlag() flag.Getter {
	tags := build.Default.ReleaseTags
	v := tags[len(tags)-1][2:]
	version := new(VersionFlag)
	if err := version.Set(v); err != nil {
		panic(fmt.Sprintf("internal error: %s", err))
	}
	return version
}

type VersionFlag int

func (v *VersionFlag) String() string {
	return fmt.Sprintf("1.%d", *v)
}

func (v *VersionFlag) Set(s string) error {
	if len(s) < 3 {
		return fmt.Errorf("invalid Go version: %q", s)
	}
	if s[0] != '1' {
		return fmt.Errorf("invalid Go version: %q", s)
	}
	if s[1] != '.' {
		return fmt.Errorf("invalid Go version: %q", s)
	}
	i, err := strconv.Atoi(s[2:])
	if err != nil {
		return fmt.Errorf("invalid Go version: %q", s)
	}
	*v = VersionFlag(i)
	return nil
}

func (v *VersionFlag) Get() interface{} {
	return int(*v)
}

// ExhaustiveTypeSwitch panics when called. It can be used to ensure
// that type switches are exhaustive.
func ExhaustiveTypeSwitch(v interface{}) {
	panic(fmt.Sprintf("internal error: unhandled case %T", v))
}

// A directive is a comment of the form '//lint:<command>
// [arguments...]'. It represents instructions to the static analysis
// tool.
type Directive struct {
	Command   string
	Arguments []string
	Directive *ast.Comment
	Node      ast.Node
}

func parseDirective(s string) (cmd string, args []string) {
	if !strings.HasPrefix(s, "//lint:") {
		return "", nil
	}
	s = strings.TrimPrefix(s, "//lint:")
	fields := strings.Split(s, " ")
	return fields[0], fields[1:]
}

// ParseDirectives extracts all directives from a list of Go files.
func ParseDirectives(files []*ast.File, fset *token.FileSet) []Directive {
	var dirs []Directive
	for _, f := range files {
		// OPT(dh): in our old code, we skip all the comment map work if we
		// couldn't find any directives, benchmark if that's actually
		// worth doing
		cm := ast.NewCommentMap(fset, f, f.Comments)
		for node, cgs := range cm {
			for _, cg := range cgs {
				for _, c := range cg.List {
					if !strings.HasPrefix(c.Text, "//lint:") {
						continue
					}
					cmd, args := parseDirective(c.Text)
					d := Directive{
						Command:   cmd,
						Arguments: args,
						Directive: c,
						Node:      node,
					}
					dirs = append(dirs, d)
				}
			}
		}
	}
	return dirs
}
