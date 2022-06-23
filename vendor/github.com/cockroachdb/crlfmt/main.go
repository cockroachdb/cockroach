// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	goparser "go/parser"
	"go/printer"
	"go/token"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/crlfmt/internal/parser"
	"github.com/cockroachdb/crlfmt/internal/render"
	"github.com/cockroachdb/gostdlib/go/format"
	"github.com/cockroachdb/gostdlib/x/tools/imports"
	"github.com/cockroachdb/ttycolor"
)

var (
	wrap         = flag.Int("wrap", 100, "column to wrap at")
	tab          = flag.Int("tab", 2, "tab width for column calculations")
	overwrite    = flag.Bool("w", false, "overwrite modified files")
	fast         = flag.Bool("fast", false, "skip running goimports and simplify")
	groupImports = flag.Bool("groupimports", true, "group imports by type")
	printDiff    = flag.Bool("diff", true, "print diffs")
	ignore       = flag.String("ignore", "", "regex matching files to skip")
	srcDir       = flag.String("srcdir", "", "resolve imports as if the source file is from the given directory (if a file is given, the parent directory is used)")
)

var (
	red   = string(ttycolor.StdoutProfile[ttycolor.Red])
	green = string(ttycolor.StdoutProfile[ttycolor.Green])
	reset = string(ttycolor.StdoutProfile[ttycolor.Reset])
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	if flag.NArg() == 0 {
		content, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}

		*overwrite = true
		*printDiff = false
		out, err := checkBuf("<standard input>", content)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(out)
		return err
	}

	if flag.NArg() > 1 {
		return errors.New("must specify exactly one path argument (or zero for stdin)")
	}

	root, err := filepath.EvalSymlinks(flag.Arg(0))
	if err != nil {
		return fmt.Errorf("following symlinks in input path: %s", err)
	}

	var ignoreRE *regexp.Regexp
	if len(*ignore) > 0 {
		ignoreRE, err = regexp.Compile(*ignore)
		if err != nil {
			return fmt.Errorf("compiling ignore regexp: %s", err)
		}
	}

	err = filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if ignoreRE != nil && ignoreRE.MatchString(path) {
			return nil
		}
		if fi.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		return checkPath(path)
	})
	if err != nil {
		return fmt.Errorf("error during walk: %s", err)
	}
	return nil
}

func checkPath(path string) error {
	src, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	output, err := checkBuf(path, src)
	if err != nil {
		return err
	}

	if !bytes.Equal(src, output) {
		if *printDiff {
			data, err := diff(src, output, path)
			if err != nil {
				return fmt.Errorf("computing diff: %s", err)
			}
			fmt.Printf("diff -u old/%[1]s new/%[1]s\n", filepath.ToSlash(path))
			os.Stdout.Write(data)
		}

		if *overwrite {
			err := ioutil.WriteFile(path, output, 0)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func checkBuf(path string, src []byte) ([]byte, error) {
	output := new(bytes.Buffer)
	if !*fast {
		// Run goimports, which also runs gofmt.
		importOpts := imports.Options{
			AllErrors:  true,
			Comments:   true,
			TabIndent:  false,
			TabWidth:   *tab,
			FormatOnly: false,
		}

		pathForImports := path
		if *srcDir != "" {
			filename := filepath.Base(path)
			if isDirectory(*srcDir) {
				pathForImports = filepath.Join(*srcDir, filename)
			} else {
				pathForImports = filepath.Join(filepath.Dir(*srcDir), filename)
			}
		}

		newSrc, err := imports.Process(pathForImports, src, &importOpts)
		if err != nil {
			return nil, err
		}
		src = newSrc

		// Simplify
		{
			fileSet := token.NewFileSet()
			f, err := goparser.ParseFile(fileSet, path, src, goparser.ParseComments)
			if err != nil {
				return nil, err
			}
			render.Simplify(f)

			prCfg := &printer.Config{
				Tabwidth: *tab,
				Mode:     printer.UseSpaces | printer.TabIndent,
			}
			var buf bytes.Buffer
			prCfg.Fprint(&buf, fileSet, f)
			src = buf.Bytes()
		}
	}

	file, err := parser.ParseFile(path, src)
	if err != nil {
		return nil, err
	}

	var importMapping map[*parser.ImportDecl][]render.ImportBlock
	if *groupImports {
		importMapping = remapImports(file)
	}

	lastPos := token.NoPos
	for _, d := range file.Decls {
		if imp, ok := d.(*parser.ImportDecl); ok && *groupImports {
			blocks := importMapping[imp]
			if blocks == nil {
				// This import declaration is meant to be removed. If it's
				// surrounded by blank lines, remove those too.
				//
				// If the import block is surrounded by blank lines, remove the
				// blank lines too.
				startPos, endPos := imp.Pos, imp.End
				if off := file.Offset(startPos); off-2 >= 0 && src[off-1] == '\n' && src[off-2] == '\n' {
					startPos = file.Pos(off - 1)
				}
				if off := file.Offset(endPos); off+1 < len(src) && src[off] == '\n' && src[off+1] == '\n' {
					endPos = file.Pos(off + 1)
				}
				output.Write(file.Slice(lastPos, startPos))
				lastPos = endPos
				continue
			}

			var importBuf bytes.Buffer
			if imp.Doc != nil && blocks[0].Size() > 1 {
				importBuf.Write(file.Slice(imp.Doc.Pos(), imp.Doc.End()))
				importBuf.WriteByte('\n')
			}
			for i, block := range blocks {
				if i > 0 {
					importBuf.WriteString("\n\n")
				}
				render.Imports(&importBuf, file, block)
			}
			newBytes, err := format.Source(importBuf.Bytes())
			if err != nil {
				return nil, fmt.Errorf("grouping imports for %s: %s", path, err)
			}
			output.Write(file.Slice(lastPos, imp.Pos))
			output.Write(newBytes)
			lastPos = imp.End
		}
		if fn, ok := d.(*parser.FuncDecl); ok {
			output.Write(file.Slice(lastPos, fn.Pos()))
			lastPos = fn.BodyEnd()
			var curFunc bytes.Buffer
			render.Func(&curFunc, file, fn, *tab, *wrap)
			output.Write(curFunc.Bytes())
		}
	}
	output.Write(src[file.Offset(lastPos):])
	return output.Bytes(), nil
}

// remapImports maps each existing import declaration in the file to an import
// block that should replace it. An import block can contain multiple import
// declarations, to indicate that the existing single import declaration should
// be replaced with multiple separate import declarations, or nil, to indicate
// that the import declaration should be removed entirely.
//
// The goal is to have just one import declaration, within which imports are
// grouped standard library imports and non-standard library imports. An
// exception is made for cgo, whose "C" psuedo-imports are extracted into
// separate import declarations.
func remapImports(file *parser.File) map[*parser.ImportDecl][]render.ImportBlock {
	var (
		stdlibImports []parser.ImportSpec
		otherImports  []parser.ImportSpec
	)

	for _, imp := range file.ImportSpecs() {
		switch impPath := imp.Path(); {
		case impPath == "C":
			continue
		case strings.Contains(impPath, "."):
			otherImports = append(otherImports, imp)
		default:
			stdlibImports = append(stdlibImports, imp)
		}
	}

	mainBlock := render.ImportBlock{stdlibImports, otherImports}
	needMainBlock := mainBlock.Size() > 0

	mapping := map[*parser.ImportDecl][]render.ImportBlock{}
	impDecls := file.ImportDecls()
	for _, imp := range impDecls {
		var blocks []render.ImportBlock
		var cImports []parser.ImportSpec
		for _, spec := range imp.Specs {
			if spec.Path() == "C" {
				cImports = append(cImports, spec)
			}
		}
		if needMainBlock && len(cImports) != len(imp.Specs) {
			// The first import declaration we see that contains something other
			// than "C" psuedo-imports will be our main import block.
			blocks = append(blocks, mainBlock)
			needMainBlock = false
		}
		// If there were any "C" psuedo-imports in this declaration, split them
		// out into their own import declarations.
		for _, imp := range cImports {
			if imp.Doc == nil {
				// A cgo import without a doc comment has no effect. Remove it.
				continue
			}
			blocks = append(blocks, render.ImportBlock{{imp}})
		}
		mapping[imp] = blocks
	}
	return mapping
}

// isDirectory returns true if the path is a directory. False is
// returned on any error.
func isDirectory(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fileInfo.IsDir()
}
