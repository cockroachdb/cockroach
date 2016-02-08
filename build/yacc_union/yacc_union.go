package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

var prefix string

var assignRe *regexp.Regexp
var dollarRe *regexp.Regexp
var appendRe *regexp.Regexp
var methodRe *regexp.Regexp

func initPrefix(p string) {
	if p == "" {
		panic("SymType prefix not found")
	}
	prefix = p
	assignRe = regexp.MustCompile(fmt.Sprintf(`%sVAL\.(\w+) = (.*)$`, p))
	appendRe = regexp.MustCompile(fmt.Sprintf(`%sVAL.+( = append\(((sqlDollar\[[0-9]+\])[^,]*), [^,]*\))$`, p))
	methodRe = regexp.MustCompile(fmt.Sprintf(`%sVAL\.([^. ]+)\.([^.]+)`, p))
	dollarRe = regexp.MustCompile(fmt.Sprintf(`(%sDollar\[[0-9]+\]\.)(\w+)`, p))
}

type typeDecl struct {
	name, typ string
}

func (td *typeDecl) prefixedName() string {
	if prefix == "" {
		panic("Cannot call prefixedName before a prefix is defined")
	}
	return prefix + strings.ToUpper(string(td.name[0])) + td.name[1:]
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	out := os.Stdout

	unionTypes := transformStructToInterface(out, in)

	typeSet := make(map[string]*typeDecl, len(unionTypes))
	for _, ty := range unionTypes {
		typeSet[ty.name] = ty
	}

	changeTypes(out, in, typeSet)
}

var symTypeRe = regexp.MustCompile(`^type (\w+)SymType struct {$`)
var typeDeclRe = regexp.MustCompile(`^\s+(\w+)\s+([^\s]+)\s*(` + "`.*`" + `)?$`)

func transformStructToInterface(w io.Writer, s *bufio.Scanner) []*typeDecl {
	var unionTypes []*typeDecl
	var nonUnionLines []string

	inUnion := false
	for s.Scan() {
		l := s.Text()
		if strings.HasPrefix(strings.TrimSpace(l), "//") {
			fmt.Fprintln(w, l)
			continue
		}

		if inUnion {
			if strings.HasPrefix(l, "}") {
				break
			}

			typeDec := typeDeclRe.FindStringSubmatch(l)
			if typeDec == nil {
				panic(fmt.Sprintf("Could not find type declaration in line %q", l))
			}

			tag := typeDec[3]
			if strings.Contains(tag, `union:"true"`) {
				name := strings.TrimSpace(typeDec[1])
				typ := strings.TrimSpace(typeDec[2])
				unionTypes = append(unionTypes, &typeDecl{name, typ})
			} else {
				nonUnionLines = append(nonUnionLines, l)
			}
		} else {
			symDec := symTypeRe.FindStringSubmatch(l)
			if symDec != nil {
				inUnion = true
				initPrefix(symDec[1])
			} else {
				fmt.Fprintln(w, l)
			}
		}
	}
	if err := s.Err(); err != nil {
		panic(err)
	}

	prefixedSymUnion := fmt.Sprintf("%sSymUnion", prefix)
	for _, ty := range unionTypes {
		fmt.Fprintf(w, "type %s struct {\n\t%s %s\n}\n", ty.prefixedName(), ty.name, ty.typ)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "type %s interface {\n\t%s()\n}\n", prefixedSymUnion, prefixedSymUnion)
	fmt.Fprintln(w)
	for _, ty := range unionTypes {
		fmt.Fprintf(w, "func (*%s) %s() {}\n", ty.prefixedName(), prefixedSymUnion)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "type %sSymType struct {\n%s\n\tunion %s\n}\n", prefix, strings.Join(nonUnionLines, "\n"), prefixedSymUnion)
	return unionTypes
}

func assignUnionInterface(s string, typeSet map[string]*typeDecl) string {
	match := assignRe.FindStringSubmatch(s)
	if match != nil {
		if typ, ok := typeSet[match[1]]; ok {
			if strings.HasPrefix(match[2], "append") {
				return fixAppending(s, typeSet)
			}
			s = strings.Replace(s, fmt.Sprintf("%sVAL.%s", prefix, match[1]), fmt.Sprintf("%sVAL.union", prefix), 1)
			s = strings.Replace(s, match[2], fmt.Sprintf("&%s{%s}", typ.prefixedName(), match[2]), 1)
		}
	}
	return s
}

var typeAssertRe = regexp.MustCompile(`^(.*?)(\.\(\w+\))?$`)

func fixAppending(s string, typeSet map[string]*typeDecl) string {
	match := appendRe.FindStringSubmatch(s)
	if match == nil {
		panic(fmt.Sprintf("Cannot transform append line: %q\n", s))
	}

	listVar := match[2]
	listMatch := typeAssertRe.FindStringSubmatch(listVar)
	if listMatch == nil {
		panic(fmt.Sprintf("Cannot determine assigning type: %q\n", listMatch))
	}
	if listMatch[2] != "" {
		listVar = listMatch[1]
	}

	first := fmt.Sprintf("%s%s; ", listVar, match[1])
	second := fmt.Sprintf("%sVAL.union = %s.union", prefix, match[3])
	return first + second
}

func fixMethodCalls(s string, typeSet map[string]*typeDecl) string {
	match := methodRe.FindStringSubmatch(s)
	if match != nil {
		typ := match[1]
		if t, ok := typeSet[typ]; ok {
			call := fmt.Sprintf("%sVAL.union.(*%s).%s.%s", prefix, t.prefixedName(), t.name, match[2])
			s = strings.Replace(s, match[0], call, 1)
		}
	}
	return s
}

func typeAssertUnions(s string, typeSet map[string]*typeDecl) string {
	matches := dollarRe.FindAllStringSubmatch(s, -1)
	for _, match := range matches {
		typ := match[2]
		if t, ok := typeSet[typ]; ok {
			s = strings.Replace(s, match[0], fmt.Sprintf("%sunion.(*%s).%s", match[1], t.prefixedName(), typ), 1)
		}
	}
	return s
}

func changeTypes(w io.Writer, s *bufio.Scanner, typeSet map[string]*typeDecl) {
	for s.Scan() {
		l := s.Text()
		if strings.HasPrefix(strings.TrimSpace(l), "//") {
			fmt.Fprintln(w, l)
			continue
		}

		if strings.Contains(l, "VAL") {
			if strings.Contains(l, " =") {
				// Make sure we have a complete variable declaration.
				open := strings.Count(l, "{")
				close := strings.Count(l, "}")
				for open != close {
					if !s.Scan() {
						panic("Could not scan while brackets are uneven")
					}
					newL := s.Text()
					open += strings.Count(newL, "{")
					close += strings.Count(newL, "}")
					l = l + newL
				}
			}

			l = assignUnionInterface(l, typeSet)
			l = fixMethodCalls(l, typeSet)
		}
		if strings.Contains(l, "Dollar[") {
			l = typeAssertUnions(l, typeSet)
		}
		fmt.Fprintln(w, l)
	}
	if err := s.Err(); err != nil {
		panic(err)
	}
}
