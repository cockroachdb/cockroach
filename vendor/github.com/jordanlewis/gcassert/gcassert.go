package gcassert

import (
	"bufio"
	"errors"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

type assertDirective int

const (
	noDirective assertDirective = iota
	inline
	bce
	noescape
)

func stringToDirective(s string) (assertDirective, error) {
	switch s {
	case "inline":
		return inline, nil
	case "bce":
		return bce, nil
	case "noescape":
		return noescape, nil
	}
	return noDirective, errors.New(fmt.Sprintf("no such directive %s", s))
}

type lineInfo struct {
	n          ast.Node
	directives []assertDirective
	// passedDirective is a map from index into the directives slice to a
	// boolean that says whether or not the directive succeeded, in the case
	// of directives like inlining that have compiler output if they passed.
	// For directives like bce that have compiler output if they failed, there's
	// no entry in this map.
	passedDirective map[int]bool
}

var gcAssertRegex = regexp.MustCompile(`//gcassert:([\w,]+)`)

type assertVisitor struct {
	commentMap ast.CommentMap

	// directiveMap is a map from line number in the source file to the AST node
	// that the line number corresponded to, as well as any directives that we
	// parsed.
	directiveMap map[int]lineInfo

	// mustInlineFuncs is a set of types.Objects that represent FuncDecls of
	// some kind that were marked with //gcassert:inline by the user.
	mustInlineFuncs map[types.Object]struct{}
	fileSet         *token.FileSet

	p *packages.Package
}

func newAssertVisitor(
	commentMap ast.CommentMap,
	fileSet *token.FileSet,
	p *packages.Package,
	mustInlineFuncs map[types.Object]struct{},
) assertVisitor {
	return assertVisitor{
		commentMap:      commentMap,
		fileSet:         fileSet,
		directiveMap:    make(map[int]lineInfo),
		mustInlineFuncs: mustInlineFuncs,
		p:               p,
	}
}

func (v assertVisitor) Visit(node ast.Node) (w ast.Visitor) {
	if node == nil {
		return w
	}
	pos := node.Pos()
	lineNumber := v.fileSet.Position(pos).Line

	m := v.commentMap[node]
	for _, g := range m {
	COMMENT_LIST:
		for _, c := range g.List {
			matches := gcAssertRegex.FindStringSubmatch(c.Text)
			if len(matches) == 0 {
				continue
			}
			// The 0th match is the whole string, and the 1st match is the
			// gcassert directive(s).
			directiveStrings := strings.Split(matches[1], ",")

			lineInfo := v.directiveMap[lineNumber]
			lineInfo.n = node
			for _, s := range directiveStrings {
				directive, err := stringToDirective(s)
				if err != nil {
					continue
				}
				if directive == inline {
					switch n := node.(type) {
					case *ast.FuncDecl:
						// Add the Object that this FuncDecl's ident is connected
						// to to our map of must-inline functions.
						obj := v.p.TypesInfo.Defs[n.Name]
						if obj != nil {
							v.mustInlineFuncs[obj] = struct{}{}
						}
						continue COMMENT_LIST
					}
				}
				lineInfo.directives = append(lineInfo.directives, directive)
			}
			v.directiveMap[lineNumber] = lineInfo
		}
	}
	return v
}

// GCAssert searches through the packages at the input path and writes failures
// to comply with //gcassert directives to the given io.Writer.
func GCAssert(w io.Writer, paths ...string) error {
	fileSet := token.NewFileSet()
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedCompiledGoFiles |
			packages.NeedTypesInfo | packages.NeedTypes,
		Fset: fileSet,
	}, paths...)
	directiveMap, err := parseDirectives(pkgs, fileSet)
	if err != nil {
		return err
	}

	// Next: invoke Go compiler with -m flags to get the compiler to print
	// its optimization decisions.

	args := []string{"build", "-gcflags=-m -m -d=ssa/check_bce/debug=1"}
	for i := range paths {
		args = append(args, "./"+paths[i])
	}
	cmd := exec.Command("go", args...)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	cmd.Dir = cwd
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw
	cmdErr := make(chan error, 1)
	go func() {
		cmdErr <- cmd.Run()
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)
	optInfo := regexp.MustCompile(`([\.\/\w]+):(\d+):\d+: (.*)`)
	boundsCheck := "Found IsInBounds"
	sliceBoundsCheck := "Found SliceIsInBounds"

	for scanner.Scan() {
		line := scanner.Text()
		matches := optInfo.FindStringSubmatch(line)
		if len(matches) != 0 {
			path := matches[1]
			lineNo, err := strconv.Atoi(matches[2])
			if err != nil {
				return err
			}
			message := matches[3]

			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			if lineToDirectives := directiveMap[absPath]; lineToDirectives != nil {
				info := lineToDirectives[lineNo]
				if len(info.directives) > 0 {
					if info.passedDirective == nil {
						info.passedDirective = make(map[int]bool)
						lineToDirectives[lineNo] = info
					}
				}
				for i, d := range info.directives {
					switch d {
					case bce:
						if message == boundsCheck || message == sliceBoundsCheck {
							// Error! We found a bounds check where the user expected
							// there to be none.
							// Print out the user's code lineNo that failed the assertion,
							// the assertion itself, and the compiler output that
							// proved that the assertion failed.
							if err := printAssertionFailure(cwd, fileSet, info, w, message); err != nil {
								return err
							}
						}
					case inline:
						if strings.HasPrefix(message, "inlining call to") {
							info.passedDirective[i] = true
						}
					case noescape:
						if strings.HasSuffix(message, "escapes to heap:") {
							if err := printAssertionFailure(cwd, fileSet, info, w, message); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}

	keys := make([]string, 0, len(directiveMap))
	for k := range directiveMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var lines []int
	for _, k := range keys {
		lines = lines[:0]
		lineToDirectives := directiveMap[k]
		for line := range lineToDirectives {
			lines = append(lines, line)
		}
		sort.Ints(lines)
		for _, line := range lines {
			info := lineToDirectives[line]
			for i, d := range info.directives {
				// An inlining directive passes if it has compiler output. For
				// each inlining directive, check if there was matching compiler
				// output and fail if not.
				if d == inline {
					if !info.passedDirective[i] {
						if err := printAssertionFailure(
							cwd, fileSet, info, w, "call was not inlined"); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func printAssertionFailure(cwd string, fileSet *token.FileSet, info lineInfo, w io.Writer, message string) error {
	var buf strings.Builder
	_ = printer.Fprint(&buf, fileSet, info.n)
	pos := fileSet.Position(info.n.Pos())
	relPath, err := filepath.Rel(cwd, pos.Filename)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s:%d:\t%s: %s\n", relPath, pos.Line, buf.String(), message)
	return nil
}

type directiveMap map[string]map[int]lineInfo

func parseDirectives(pkgs []*packages.Package, fileSet *token.FileSet) (directiveMap, error) {
	fileDirectiveMap := make(directiveMap)
	mustInlineFuncs := make(map[types.Object]struct{})
	for _, pkg := range pkgs {
		for i, file := range pkg.Syntax {
			commentMap := ast.NewCommentMap(fileSet, file, file.Comments)

			v := newAssertVisitor(commentMap, fileSet, pkg, mustInlineFuncs)
			// First: find all lines of code annotated with our gcassert directives.
			ast.Walk(v, file)

			file := pkg.CompiledGoFiles[i]
			if len(v.directiveMap) > 0 {
				fileDirectiveMap[file] = v.directiveMap
			}
		}
	}

	// Do another pass to find all callsites of funcs marked with inline.
	for _, pkg := range pkgs {
		for i, file := range pkg.Syntax {
			v := &inlinedDeclVisitor{assertVisitor: newAssertVisitor(nil, fileSet, pkg, mustInlineFuncs)}
			filePath := pkg.CompiledGoFiles[i]
			v.directiveMap = fileDirectiveMap[filePath]
			if v.directiveMap == nil {
				v.directiveMap = make(map[int]lineInfo)
			}
			ast.Walk(v, file)
			if len(v.directiveMap) > 0 {
				fileDirectiveMap[filePath] = v.directiveMap
			}
		}
	}
	return fileDirectiveMap, nil
}

type inlinedDeclVisitor struct {
	assertVisitor
}

func (v *inlinedDeclVisitor) Visit(node ast.Node) (w ast.Visitor) {
	if node == nil {
		return w
	}
	pos := node.Pos()
	lineNumber := v.fileSet.Position(pos).Line

	// Search for all func callsites of functions that were marked with
	// gcassert:inline and add inline directives to those callsites.
	switch n := node.(type) {
	case *ast.CallExpr:
		var obj types.Object
		switch n := n.Fun.(type) {
		case *ast.Ident:
			obj = v.p.TypesInfo.Uses[n]
		case *ast.SelectorExpr:
			sel := v.p.TypesInfo.Selections[n]
			if sel == nil {
				break
			}
			obj = sel.Obj()
		}
		if _, ok := v.mustInlineFuncs[obj]; ok {
			lineInfo := v.directiveMap[lineNumber]
			lineInfo.n = node
			lineInfo.directives = append(lineInfo.directives, inline)
			v.directiveMap[lineNumber] = lineInfo
		}
	}
	return v
}
