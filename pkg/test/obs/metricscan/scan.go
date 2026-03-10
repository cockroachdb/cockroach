// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package metricscan provides AST-based scanning for metric.Metadata
// definitions in CockroachDB Go source files. It builds a mapping
// from Prometheus-format metric names to their source file locations.
package metricscan

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// prometheusNameRE matches characters that are not valid in Prometheus
// metric names. This mirrors metric.ExportedName in
// pkg/util/metric/registry.go.
var prometheusNameRE = regexp.MustCompile("[^a-zA-Z0-9_:]")

// ExportedName converts a CRDB metric name (e.g. "sql.bytesout") to
// its Prometheus-exported form (e.g. "sql_bytesout"). This is the
// same transformation as metric.ExportedName.
func ExportedName(name string) string {
	return prometheusNameRE.ReplaceAllString(name, "_")
}

// formatVerbRE detects Go format verbs (%s, %d, %v, etc.) in strings.
var formatVerbRE = regexp.MustCompile(`%[sdvfgtqxXoObBeEpTwU]`)

// Source records where a metric.Metadata Name was defined.
type Source struct {
	File string // repo-relative path, e.g. "pkg/kv/kvprober/kvprober.go"
	Line int
}

type sprintfPattern struct {
	format string
	re     *regexp.Regexp
	source Source
}

// Result holds everything the AST scanner found. All names are stored
// in Prometheus-exported form (underscores) so they can be matched
// directly against scraped names.
type Result struct {
	// Exact maps a Prometheus-format metric name to its definition
	// site. Built from both Name and LabeledName fields.
	Exact map[string]Source
	// Patterns holds fmt.Sprintf-based dynamic metric names, with
	// regexps compiled against Prometheus-format names.
	Patterns []sprintfPattern
	// Prefixes holds metric name prefixes (stored in Prometheus
	// form) used with concatenation, e.g. admission work queues.
	Prefixes []struct {
		Prefix string
		Source Source
	}
	// ParseErrors records files that could not be parsed. The scan
	// continues past parse errors; callers can inspect this list to
	// decide whether to fail.
	ParseErrors []error
}

// Scan walks all .go files under pkgDir and builds a map of metric
// Name definitions. It looks for composite literals of type
// metric.Metadata and extracts the Name and LabeledName field values.
//
// All names are converted to Prometheus-exported form (dots/dashes
// become underscores) so they can be directly compared against
// scraped Prometheus names without lossy reverse conversion.
func Scan(repoRoot string) (*Result, error) {
	pkgDir := filepath.Join(repoRoot, "pkg")
	result := &Result{
		Exact: make(map[string]Source),
	}

	err := filepath.WalkDir(pkgDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := d.Name()
			if base == "vendor" || base == "testdata" || base == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasSuffix(path, ".pb.go") || strings.HasSuffix(path, ".pb.gw.go") {
			return nil
		}
		relPath, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return fmt.Errorf("filepath.Rel failed for %s: %w", path, err)
		}
		if strings.HasPrefix(relPath, filepath.Join("pkg", "cmd", "roachprod-centralized")) {
			return nil
		}

		if err := scanFile(path, relPath, result); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func scanFile(path, relPath string, result *Result) error {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		// Some files may fail to parse due to build tags or
		// generated code. Record the error but continue
		// scanning. The caller (e.g. test) can inspect
		// ParseErrors to decide whether to fail.
		result.ParseErrors = append(result.ParseErrors,
			fmt.Errorf("failed to parse %s: %w", path, err))
		return nil
	}

	metricAlias := findMetricAlias(f)
	if metricAlias == "" {
		return nil
	}

	// variableNameFuncs tracks functions where a metric.Metadata
	// Name field is set to a function parameter.
	variableNameFuncs := make(map[string]int)

	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if !isMetricMetadata(cl, metricAlias) {
			return true
		}

		pos := fset.Position(cl.Pos())
		src := Source{File: relPath, Line: pos.Line}

		if nameExpr := findField(cl, "Name"); nameExpr != nil {
			if ident, isIdent := nameExpr.(*ast.Ident); isIdent {
				if fnName, paramIdx := findEnclosingFuncParam(f, cl, ident.Name); fnName != "" {
					variableNameFuncs[fnName] = paramIdx
				}
			}
			recordName(nameExpr, src, result)
		}

		if labeledExpr := findField(cl, "LabeledName"); labeledExpr != nil {
			recordName(labeledExpr, src, result)
		}

		return true
	})

	if len(variableNameFuncs) > 0 {
		src := Source{File: relPath, Line: 0}
		resolveCallSiteNames(f, variableNameFuncs, src, result)
	}

	return nil
}

func recordName(expr ast.Expr, src Source, result *Result) {
	switch v := expr.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return
		}
		name, err := strconv.Unquote(v.Value)
		if err != nil || name == "" {
			return
		}
		if formatVerbRE.MatchString(name) {
			re := formatToPromRegexp(name)
			if re != nil {
				result.Patterns = append(result.Patterns, sprintfPattern{
					format: name,
					re:     re,
					source: src,
				})
			}
		} else if strings.HasSuffix(name, ".") {
			result.Prefixes = append(result.Prefixes, struct {
				Prefix string
				Source Source
			}{Prefix: ExportedName(name), Source: src})
		} else {
			result.Exact[ExportedName(name)] = src
		}
	case *ast.BinaryExpr:
		if v.Op == token.ADD {
			if lit, ok := v.X.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				prefix, err := strconv.Unquote(lit.Value)
				if err == nil && prefix != "" {
					result.Prefixes = append(result.Prefixes, struct {
						Prefix string
						Source Source
					}{Prefix: ExportedName(prefix), Source: src})
				}
			}
		}
	case *ast.CallExpr:
		pattern := extractSprintfPattern(v)
		if pattern != "" {
			re := formatToPromRegexp(pattern)
			if re != nil {
				result.Patterns = append(result.Patterns, sprintfPattern{
					format: pattern,
					re:     re,
					source: src,
				})
			}
		}
	}
}

func findMetricAlias(f *ast.File) string {
	const metricPkg = "github.com/cockroachdb/cockroach/pkg/util/metric"
	for _, imp := range f.Imports {
		impPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		if impPath == metricPkg {
			if imp.Name != nil {
				if imp.Name.Name == "_" || imp.Name.Name == "." {
					return ""
				}
				return imp.Name.Name
			}
			return "metric"
		}
	}
	return ""
}

func isMetricMetadata(cl *ast.CompositeLit, alias string) bool {
	sel, ok := cl.Type.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == alias && sel.Sel.Name == "Metadata"
}

func findField(cl *ast.CompositeLit, fieldName string) ast.Expr {
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		ident, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		if ident.Name == fieldName {
			return kv.Value
		}
	}
	return nil
}

func extractSprintfPattern(call *ast.CallExpr) string {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return ""
	}
	if ident.Name != "fmt" || sel.Sel.Name != "Sprintf" {
		return ""
	}
	if len(call.Args) == 0 {
		return ""
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return ""
	}
	return s
}

func formatToPromRegexp(format string) *regexp.Regexp {
	parts := strings.Split(format, "%")
	var buf strings.Builder
	buf.WriteString("^")
	for i, part := range parts {
		if i == 0 {
			buf.WriteString(regexp.QuoteMeta(ExportedName(part)))
			continue
		}
		if len(part) == 0 {
			buf.WriteString("%")
			continue
		}
		verb := part[0]
		rest := part[1:]
		switch verb {
		case 'd':
			buf.WriteString("[0-9]+")
		default:
			buf.WriteString("[a-zA-Z0-9_]+")
		}
		buf.WriteString(regexp.QuoteMeta(ExportedName(rest)))
	}
	buf.WriteString("$")
	re, err := regexp.Compile(buf.String())
	if err != nil {
		return nil
	}
	return re
}

func findEnclosingFuncParam(
	f *ast.File, target ast.Node, paramName string,
) (funcName string, paramIdx int) {
	var enclosing ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			return false
		}
		switch fn := n.(type) {
		case *ast.FuncDecl:
			if containsNode(fn, target) {
				enclosing = fn
			}
		case *ast.FuncLit:
			if containsNode(fn, target) {
				enclosing = fn
			}
		}
		return true
	})

	switch fn := enclosing.(type) {
	case *ast.FuncDecl:
		if idx := paramIndex(fn.Type, paramName); idx >= 0 {
			return fn.Name.Name, idx
		}
	case *ast.FuncLit:
		if idx := paramIndex(fn.Type, paramName); idx >= 0 {
			name := findFuncLitName(f, fn)
			if name != "" {
				return name, idx
			}
		}
	}
	return "", -1
}

func containsNode(parent, target ast.Node) bool {
	found := false
	ast.Inspect(parent, func(n ast.Node) bool {
		if n == target {
			found = true
			return false
		}
		return !found
	})
	return found
}

func paramIndex(ft *ast.FuncType, name string) int {
	if ft.Params == nil {
		return -1
	}
	idx := 0
	for _, field := range ft.Params.List {
		for _, ident := range field.Names {
			if ident.Name == name {
				return idx
			}
			idx++
		}
		if len(field.Names) == 0 {
			idx++
		}
	}
	return -1
}

func findFuncLitName(f *ast.File, lit *ast.FuncLit) string {
	var name string
	ast.Inspect(f, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return name == ""
		}
		for i, rhs := range assign.Rhs {
			if rhs == lit && i < len(assign.Lhs) {
				if ident, ok := assign.Lhs[i].(*ast.Ident); ok {
					name = ident.Name
				}
			}
		}
		return name == ""
	})
	return name
}

func resolveCallSiteNames(
	f *ast.File, variableNameFuncs map[string]int, src Source, result *Result,
) {
	for {
		newFuncs := make(map[string]int)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var calledName string
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				calledName = fn.Name
			case *ast.SelectorExpr:
				calledName = fn.Sel.Name
			}
			paramIdx, ok := variableNameFuncs[calledName]
			if !ok || paramIdx >= len(call.Args) {
				return true
			}
			arg := call.Args[paramIdx]
			if ident, ok := arg.(*ast.Ident); ok {
				if fnName, pIdx := findEnclosingFuncParam(f, call, ident.Name); fnName != "" {
					if _, already := variableNameFuncs[fnName]; !already {
						newFuncs[fnName] = pIdx
					}
				}
			}
			extractMetricNames(f, arg, src, result)
			return true
		})
		if len(newFuncs) == 0 {
			break
		}
		for k, v := range newFuncs {
			variableNameFuncs[k] = v
		}
	}
}

func extractMetricNames(f *ast.File, expr ast.Expr, src Source, result *Result) {
	switch v := expr.(type) {
	case *ast.BasicLit, *ast.CallExpr:
		recordName(expr, src, result)
	case *ast.BinaryExpr:
		if v.Op == token.ADD {
			extractMetricNames(f, v.X, src, result)
			extractMetricNames(f, v.Y, src, result)
		}
	case *ast.Ident:
		ast.Inspect(f, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, lhs := range assign.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == v.Name && i < len(assign.Rhs) {
					extractMetricNames(f, assign.Rhs[i], src, result)
				}
			}
			return true
		})
	}
}

// Resolve tries to find the source file where a scraped Prometheus
// metric name was defined. All comparisons happen in Prometheus name
// space (underscores).
func (r *Result) Resolve(promName string) (Source, bool) {
	if src, ok := r.Exact[promName]; ok {
		return src, true
	}

	baseName := StripHistogramSuffix(promName)
	if baseName != promName {
		if src, ok := r.Exact[baseName]; ok {
			return src, true
		}
	}

	for _, p := range r.Prefixes {
		if strings.HasPrefix(promName, p.Prefix) {
			return p.Source, true
		}
		if baseName != promName && strings.HasPrefix(baseName, p.Prefix) {
			return p.Source, true
		}
	}

	for _, p := range r.Patterns {
		if p.re.MatchString(promName) {
			return p.source, true
		}
		if baseName != promName && p.re.MatchString(baseName) {
			return p.source, true
		}
	}

	for i := len(promName) - 1; i >= 0; i-- {
		if promName[i] == '_' {
			candidate := promName[:i]
			if src, ok := r.Exact[candidate]; ok {
				return src, true
			}
		}
	}

	if strings.HasSuffix(baseName, "_internal") {
		stripped := strings.TrimSuffix(baseName, "_internal")
		if src, ok := r.Exact[stripped]; ok {
			return src, true
		}
	}

	return Source{}, false
}

// HistogramSuffixes are the Prometheus-convention suffixes appended
// to histogram metric names when exported in text format. Note:
// _total is intentionally excluded — it is a counter convention
// and stripping it would false-match metrics whose CRDB name
// genuinely ends in "_total" (e.g. sql_distsql_flows_total).
var HistogramSuffixes = []string{
	"_bucket",
	"_sum",
	"_count",
}

// StripHistogramSuffix removes Prometheus histogram suffixes from a
// metric name if present.
func StripHistogramSuffix(name string) string {
	for _, suffix := range HistogramSuffixes {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix)
		}
	}
	return name
}

// MarshalText serializes the scan result as a simple text format:
// one "promName\tfile:line" per line for exact matches, then
// "P\tformat\tfile:line" for patterns, then "X\tprefix\tfile:line"
// for prefixes.
func (r *Result) MarshalText() []byte {
	var buf strings.Builder
	for name, src := range r.Exact {
		fmt.Fprintf(&buf, "%s\t%s:%d\n", name, src.File, src.Line)
	}
	for _, p := range r.Patterns {
		fmt.Fprintf(&buf, "P\t%s\t%s:%d\n", p.format, p.source.File, p.source.Line)
	}
	for _, p := range r.Prefixes {
		fmt.Fprintf(&buf, "X\t%s\t%s:%d\n", p.Prefix, p.Source.File, p.Source.Line)
	}
	return []byte(buf.String())
}

// UnmarshalText parses the text format produced by MarshalText.
func UnmarshalText(data []byte) (*Result, error) {
	result := &Result{Exact: make(map[string]Source)}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) == 2 {
			// Exact match: "promName\tfile:line"
			result.Exact[parts[0]] = parseSource(parts[1])
		} else if len(parts) == 3 && parts[0] == "P" {
			// Pattern: "P\tformat\tfile:line"
			re := formatToPromRegexp(parts[1])
			if re != nil {
				result.Patterns = append(result.Patterns, sprintfPattern{
					format: parts[1],
					re:     re,
					source: parseSource(parts[2]),
				})
			}
		} else if len(parts) == 3 && parts[0] == "X" {
			// Prefix: "X\tprefix\tfile:line"
			result.Prefixes = append(result.Prefixes, struct {
				Prefix string
				Source Source
			}{Prefix: parts[1], Source: parseSource(parts[2])})
		}
	}
	return result, nil
}

func parseSource(s string) Source {
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return Source{File: s}
	}
	line, err := strconv.Atoi(s[idx+1:])
	if err != nil {
		return Source{File: s}
	}
	return Source{File: s[:idx], Line: line}
}
