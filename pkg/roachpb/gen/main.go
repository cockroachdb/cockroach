// Copyright 2016 The Cockroach Authors.
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
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/ast/inspector"
)

type variantInfo struct {
	// variantType is the name of the variant type that implements
	// the union interface (isRequestUnion_Value,isResponseUnion_Value).
	variantType string
	// variantName is the unique suffix of variantType. It is also
	// the name of the single field in this type.
	variantName string
	// msgType is the name of the variant's corresponding Request/Response
	// type.
	msgType string
}

var errVariants []variantInfo
var reqVariants []variantInfo
var resVariants []variantInfo
var reqResVariantMapping map[variantInfo]variantInfo

func initVariant(ins *inspector.Inspector, varName string) variantInfo {
	fieldName, msgName := findVariantField(ins, varName)
	return variantInfo{
		variantType: varName,
		variantName: fieldName,
		msgType:     msgName,
	}
}

func initVariants(ins *inspector.Inspector) {
	errVars := findVariantTypes(ins, "ErrorDetail")
	for _, v := range errVars {
		errInfo := initVariant(ins, v)
		errVariants = append(errVariants, errInfo)
	}

	resVars := findVariantTypes(ins, "ResponseUnion")
	resVarInfos := make(map[string]variantInfo, len(resVars))
	for _, v := range resVars {
		resInfo := initVariant(ins, v)
		resVariants = append(resVariants, resInfo)
		resVarInfos[resInfo.variantName] = resInfo
	}

	reqVars := findVariantTypes(ins, "RequestUnion")
	reqResVariantMapping = make(map[variantInfo]variantInfo, len(reqVars))
	for _, v := range reqVars {
		reqInfo := initVariant(ins, v)
		reqVariants = append(reqVariants, reqInfo)

		// The ResponseUnion variants match those in RequestUnion, with the
		// following exceptions:
		resName := reqInfo.variantName
		switch resName {
		case "TransferLease":
			resName = "RequestLease"
		}
		resInfo, ok := resVarInfos[resName]
		if !ok {
			panic(fmt.Sprintf("unknown response variant %q", resName))
		}
		reqResVariantMapping[reqInfo] = resInfo
	}
}

// findVariantTypes leverages the fact that the protobuf generations creates
// a method called XXX_OneofWrappers for oneof message types. The body of that
// method is always a single return expression which returns a []interface{}
// where each of the element in the slice literal are (*<type>)(nil)
// expressions which can be interpreted using reflection. We'll find that
// method for the oneof type with the requested name and then fish out the
// list of variant types from inside that ParenExpr in the CompositeLit
// underneath that method.
//
// The code in question looks like the below snippet, where we would pull
// "ErrorDetail_NotLeaseholder" one of the returned strings.
//
//  // XXX_OneofWrappers is for the internal use of the proto package.
//  func (*ErrorDetail) XXX_OneofWrappers() []interface{} {
//  return []interface{}{
//    (*ErrorDetail_NotLeaseHolder)(nil),
//    ...
//
func findVariantTypes(ins *inspector.Inspector, oneofName string) []string {
	var variants []string
	var inFunc bool
	var inParen bool
	ins.Nodes([]ast.Node{
		(*ast.FuncDecl)(nil),
		(*ast.ParenExpr)(nil),
		(*ast.Ident)(nil),
	}, func(node ast.Node, push bool) (proceed bool) {
		switch n := node.(type) {
		case *ast.FuncDecl:
			if n.Name.Name != "XXX_OneofWrappers" {
				return false
			}
			se, ok := n.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				return false
			}
			if se.X.(*ast.Ident).Name != oneofName {
				return false
			}
			inFunc = push
			return true
		case *ast.ParenExpr:
			inParen = push && inFunc
			return inParen
		case *ast.Ident:
			if inParen {
				variants = append(variants, n.Name)
			}
			return false
		default:
			return false
		}
	})
	return variants
}

// findVariantField is used to find the field name and type name of the
// variant with the name vType. Oneof variant types always have a single
// field.
//
// The code in question looks like the below snippet, where we would return
// ("NotLeaseHolder", "NotLeaseHolderError").
//
//  type ErrorDetail_NotLeaseHolder struct {
//    NotLeaseHolder *NotLeaseHolderError
//  }
//
func findVariantField(ins *inspector.Inspector, vType string) (fieldName, msgName string) {
	ins.Preorder([]ast.Node{
		(*ast.TypeSpec)(nil),
	}, func(node ast.Node) {
		n := node.(*ast.TypeSpec)
		if n.Name.Name != vType {
			return
		}
		st, ok := n.Type.(*ast.StructType)
		if !ok {
			return
		}
		if len(st.Fields.List) != 1 {
			panic(fmt.Sprintf("type %v has %d fields", vType, len(st.Fields.List)))
		}
		f := st.Fields.List[0]
		fieldName = f.Names[0].Name
		msgName = f.Type.(*ast.StarExpr).X.(*ast.Ident).Name
	})
	return fieldName, msgName
}

func genGetInner(w io.Writer, unionName, variantName string, variants []variantInfo) {
	fmt.Fprintf(w, `
// GetInner returns the %[2]s contained in the union.
func (ru %[1]s) GetInner() %[2]s {
	switch t := ru.GetValue().(type) {
`, unionName, variantName)

	for _, v := range variants {
		fmt.Fprintf(w, `	case *%s:
		return t.%s
`, v.variantType, v.variantName)
	}

	fmt.Fprint(w, `	default:
		return nil
	}
}
`)
}

func genMustSetInner(w io.Writer, unionName, variantName string, variants []variantInfo) {
	fmt.Fprintf(w, `
// MustSetInner sets the %[2]s in the union.
func (ru *%[1]s) MustSetInner(r %[2]s) {
	ru.Reset()
	var union is%[1]s_Value
	switch t := r.(type) {
`, unionName, variantName)

	for _, v := range variants {
		fmt.Fprintf(w, `	case *%s:
		union = &%s{t}
`, v.msgType, v.variantType)
	}

	fmt.Fprintf(w, `	default:
		panic(fmt.Sprintf("unsupported type %%T for %%T", r, ru))
	}
	ru.Value = union
}
`)
}

func main() {
	var filenameFlag = flag.String("filename", "batch_generated.go", "filename path")
	flag.Parse()
	fileSet := token.NewFileSet()
	var files []*ast.File
	for _, arg := range flag.Args() {
		expanded, err := filepath.Glob(arg)
		if err != nil {
			panic(fmt.Sprintf("failed to expand %s: %v", arg, err))
		}
		for _, fp := range expanded {
			f, err := parser.ParseFile(fileSet, fp, nil, 0)
			if err != nil {
				panic(fmt.Sprintf("failed to parse %s: %v", arg, err))
			}
			files = append(files, f)
		}
	}

	ins := inspector.New(files)
	initVariants(ins)

	f, err := os.Create(*filenameFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening file: ", err)
		os.Exit(1)
	}

	// First comment for github/Go; second for reviewable.
	// https://github.com/golang/go/issues/13560#issuecomment-277804473
	// https://github.com/Reviewable/Reviewable/wiki/FAQ#how-do-i-tell-reviewable-that-a-file-is-generated-and-should-not-be-reviewed
	fmt.Fprint(f, `// Code generated by genbatch/main.go; DO NOT EDIT.
// GENERATED FILE DO NOT EDIT

package roachpb

import (
	"fmt"
	"strconv"
	"strings"
)
`)

	// Generate GetInner methods.
	genGetInner(f, "ErrorDetail", "error", errVariants)
	genGetInner(f, "RequestUnion", "Request", reqVariants)
	genGetInner(f, "ResponseUnion", "Response", resVariants)

	// Generate MustSetInner methods.
	genMustSetInner(f, "ErrorDetail", "error", errVariants)
	genMustSetInner(f, "RequestUnion", "Request", reqVariants)
	genMustSetInner(f, "ResponseUnion", "Response", resVariants)

	fmt.Fprintf(f, `
type reqCounts [%d]int32
`, len(reqVariants))

	// Generate getReqCounts function.
	fmt.Fprint(f, `
// getReqCounts returns the number of times each
// request type appears in the batch.
func (ba *BatchRequest) getReqCounts() reqCounts {
	var counts reqCounts
	for _, ru := range ba.Requests {
		switch ru.GetValue().(type) {
`)

	for i, v := range reqVariants {
		fmt.Fprintf(f, `		case *%s:
			counts[%d]++
`, v.variantType, i)
	}

	fmt.Fprintf(f, `		default:
			panic(fmt.Sprintf("unsupported request: %%+v", ru))
		}
	}
	return counts
}
`)

	// A few shorthands to help make the names more terse.
	shorthands := map[string]string{
		"Delete":      "Del",
		"Range":       "Rng",
		"Transaction": "Txn",
		"Reverse":     "Rev",
		"Admin":       "Adm",
		"Increment":   "Inc",
		"Conditional": "C",
		"Check":       "Chk",
		"Truncate":    "Trunc",
	}

	// Generate Summary function.
	fmt.Fprintf(f, `
var requestNames = []string{`)
	for _, v := range reqVariants {
		name := v.variantName
		for str, short := range shorthands {
			name = strings.Replace(name, str, short, -1)
		}
		fmt.Fprintf(f, `
	"%s",`, name)
	}
	fmt.Fprint(f, `
}
`)

	// We don't use Fprint to avoid go vet warnings about
	// formatting directives in string.
	fmt.Fprint(f, `
// Summary prints a short summary of the requests in a batch.
func (ba *BatchRequest) Summary() string {
	var b strings.Builder
	ba.WriteSummary(&b)
	return b.String()
}

// WriteSummary writes a short summary of the requests in a batch
// to the provided builder.
func (ba *BatchRequest) WriteSummary(b *strings.Builder) {
	if len(ba.Requests) == 0 {
		b.WriteString("empty batch")
		return
	}
	counts := ba.getReqCounts()
	var tmp [10]byte
	var comma bool
	for i, v := range counts {
		if v != 0 {
			if comma {
				b.WriteString(", ")
			}
			comma = true

			b.Write(strconv.AppendInt(tmp[:0], int64(v), 10))
			b.WriteString(" ")
			b.WriteString(requestNames[i])
		}
	}
}
`)

	// Generate CreateReply function.
	fmt.Fprint(f, `
// The following types are used to group the allocations of Responses
// and their corresponding isResponseUnion_Value union wrappers together.
`)
	allocTypes := make(map[string]string)
	for _, resV := range resVariants {
		allocName := strings.ToLower(resV.msgType[:1]) + resV.msgType[1:] + "Alloc"
		fmt.Fprintf(f, `type %s struct {
	union %s
	resp  %s
}
`, allocName, resV.variantType, resV.msgType)
		allocTypes[resV.variantName] = allocName
	}

	fmt.Fprint(f, `
// CreateReply creates replies for each of the contained requests, wrapped in a
// BatchResponse. The response objects are batch allocated to minimize
// allocation overhead.
func (ba *BatchRequest) CreateReply() *BatchResponse {
	br := &BatchResponse{}
	br.Responses = make([]ResponseUnion, len(ba.Requests))

	counts := ba.getReqCounts()

`)

	for i, v := range reqVariants {
		resV, ok := reqResVariantMapping[v]
		if !ok {
			panic(fmt.Sprintf("unknown response variant for %v", v))
		}
		fmt.Fprintf(f, "	var buf%d []%s\n", i, allocTypes[resV.variantName])
	}

	fmt.Fprint(f, `
	for i, r := range ba.Requests {
		switch r.GetValue().(type) {
`)

	for i, v := range reqVariants {
		resV, ok := reqResVariantMapping[v]
		if !ok {
			panic(fmt.Sprintf("unknown response variant for %v", v))
		}

		fmt.Fprintf(f, `		case *%[2]s:
			if buf%[1]d == nil {
				buf%[1]d = make([]%[3]s, counts[%[1]d])
			}
			buf%[1]d[0].union.%[4]s = &buf%[1]d[0].resp
			br.Responses[i].Value = &buf%[1]d[0].union
			buf%[1]d = buf%[1]d[1:]
`, i, v.variantType, allocTypes[resV.variantName], resV.variantName)
	}

	fmt.Fprintf(f, "%s", `		default:
			panic(fmt.Sprintf("unsupported request: %+v", r))
		}
	}
	return br
}
`)

	fmt.Fprint(f, `
// CreateRequest creates an empty Request for each of the Method types.
func CreateRequest(method Method) Request {
	switch method {`)
	for _, v := range reqVariants {
		fmt.Fprintf(f, `
	case %s:
		return &%s{}`, v.msgType[:len(v.msgType)-7], v.msgType)
	}
	fmt.Fprintf(f, "%s", `
	default:
		panic(fmt.Sprintf("unsupported method: %+v", method))
	}
}
`)

	if err := f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "Error closing file: ", err)
		os.Exit(1)
	}
}
