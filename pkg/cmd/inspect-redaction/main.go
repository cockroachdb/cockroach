package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/tools/go/packages"
)

type typeInfo struct {
	location string
	logFunc  string
}

type typeInfos []typeInfo

func (t typeInfos) String() string {
	l := []string{}
	for _, info := range t {
		l = append(l, fmt.Sprintf("%s(%s)", info.location, info.logFunc))
	}

	return strings.Join(l, ", ")
}

var (
	reFuncPattern = regexp.MustCompile(`^(Infof|Debugf|Errorf|Warnf|Info|Debug|Error|Warn)$`)
	patterns      = os.Args[1:]
	numOfWorkers  = min(runtime.NumCPU(), len(patterns))

	loggedTypes = struct {
		sync.RWMutex
		types map[types.Type]typeInfos
	}{types: make(map[types.Type]typeInfos)}

	safeMethodLookup = map[string]struct{}{
		"SafeFormat": {},
		"SafeValue":  {},
	}
)

func main() {
	cfg := &packages.Config{
		Mode: packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}

	var (
		wg       = sync.WaitGroup{}
		currDir  = os.Getenv("PWD")
		workChan = make(chan string, len(patterns))
	)

	for i := 0; i < numOfWorkers; i++ {
		go func(id int) {
			for pattern := range workChan {
				pkgs, err := packages.Load(cfg, pattern)
				if err != nil {
					panic(err)
				}

				for _, pkg := range pkgs {
					for _, file := range pkg.Syntax {
						ast.Inspect(file, func(n ast.Node) bool {
							node, ok := n.(*ast.CallExpr)
							if !ok {
								return true
							}

							fun, ok := node.Fun.(*ast.SelectorExpr)
							if !ok {
								return true
							}

							if reFuncPattern.MatchString(fun.Sel.Name) {
								fileName := strings.ReplaceAll(pkg.Fset.Position(node.Pos()).Filename, currDir, "")
								loc := fmt.Sprintf(".%s:%d", fileName, pkg.Fset.Position(node.Pos()).Line)

								loggedTypes.Lock()
								defer loggedTypes.Unlock()

								if len(node.Args) == 0 {
									return true
								}

								for _, arg := range node.Args[1:] {
									arr, ok := loggedTypes.types[pkg.TypesInfo.TypeOf(arg)]
									if !ok {
										arr = []typeInfo{}
									}

									arr = append(arr, typeInfo{location: loc, logFunc: fun.Sel.Name})
									loggedTypes.types[pkg.TypesInfo.TypeOf(arg)] = arr
								}
							}

							return true
						})
					}

					fmt.Fprintf(os.Stderr, "Processed %s\n", pkg.ID)
				}

				wg.Done()
			}
		}(i)
	}

	wg.Add(len(patterns))
	for _, pattern := range patterns {
		workChan <- pattern
	}

	wg.Wait()
	close(workChan)

	check := typeChecker()
	fmt.Println("Type\tRedacted correctly\tUsage")
	for typ, info := range loggedTypes.types {
		isSafe := check(typ)
		fmt.Printf("%s\t%t\t%s\n", typ, isSafe, info)
	}
}

func typeChecker() func(types.Type) bool {
	typeState := make(map[types.Type]bool)
	return func(typ types.Type) bool {
		if isSafe, ok := typeState[typ]; ok {
			return isSafe
		}

		if named, ok := typ.(*types.Named); ok {
			for i := 0; i < named.NumMethods(); i++ {
				if _, ok := safeMethodLookup[named.Method(i).Name()]; ok {
					typeState[typ] = true
					return true
				}
			}
		}

		typeState[typ] = false
		return false
	}
}
