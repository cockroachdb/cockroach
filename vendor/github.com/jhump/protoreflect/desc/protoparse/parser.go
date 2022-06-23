package protoparse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/internal"
	"github.com/jhump/protoreflect/desc/protoparse/ast"
)

//go:generate goyacc -o proto.y.go -p proto proto.y

func init() {
	protoErrorVerbose = true

	// fix up the generated "token name" array so that error messages are nicer
	setTokenName(_STRING_LIT, "string literal")
	setTokenName(_INT_LIT, "int literal")
	setTokenName(_FLOAT_LIT, "float literal")
	setTokenName(_NAME, "identifier")
	setTokenName(_ERROR, "error")
	// for keywords, just show the keyword itself wrapped in quotes
	for str, i := range keywords {
		setTokenName(i, fmt.Sprintf(`"%s"`, str))
	}
}

func setTokenName(token int, text string) {
	// NB: this is based on logic in generated parse code that translates the
	// int returned from the lexer into an internal token number.
	var intern int
	if token < len(protoTok1) {
		intern = protoTok1[token]
	} else {
		if token >= protoPrivate {
			if token < protoPrivate+len(protoTok2) {
				intern = protoTok2[token-protoPrivate]
			}
		}
		if intern == 0 {
			for i := 0; i+1 < len(protoTok3); i += 2 {
				if protoTok3[i] == token {
					intern = protoTok3[i+1]
					break
				}
			}
		}
	}

	if intern >= 1 && intern-1 < len(protoToknames) {
		protoToknames[intern-1] = text
		return
	}

	panic(fmt.Sprintf("Unknown token value: %d", token))
}

// FileAccessor is an abstraction for opening proto source files. It takes the
// name of the file to open and returns either the input reader or an error.
type FileAccessor func(filename string) (io.ReadCloser, error)

// FileContentsFromMap returns a FileAccessor that uses the given map of file
// contents. This allows proto source files to be constructed in memory and
// easily supplied to a parser. The map keys are the paths to the proto source
// files, and the values are the actual proto source contents.
func FileContentsFromMap(files map[string]string) FileAccessor {
	return func(filename string) (io.ReadCloser, error) {
		contents, ok := files[filename]
		if !ok {
			return nil, os.ErrNotExist
		}
		return ioutil.NopCloser(strings.NewReader(contents)), nil
	}
}

// Parser parses proto source into descriptors.
type Parser struct {
	// The paths used to search for dependencies that are referenced in import
	// statements in proto source files. If no import paths are provided then
	// "." (current directory) is assumed to be the only import path.
	//
	// This setting is only used during ParseFiles operations. Since calls to
	// ParseFilesButDoNotLink do not link, there is no need to load and parse
	// dependencies.
	ImportPaths []string

	// If true, the supplied file names/paths need not necessarily match how the
	// files are referenced in import statements. The parser will attempt to
	// match import statements to supplied paths, "guessing" the import paths
	// for the files. Note that this inference is not perfect and link errors
	// could result. It works best when all proto files are organized such that
	// a single import path can be inferred (e.g. all files under a single tree
	// with import statements all being relative to the root of this tree).
	InferImportPaths bool

	// LookupImport is a function that accepts a filename and
	// returns a file descriptor, which will be consulted when resolving imports.
	// This allows a compiled Go proto in another Go module to be referenced
	// in the proto(s) being parsed.
	//
	// In the event of a filename collision, Accessor is consulted first,
	// then LookupImport is consulted, and finally the well-known protos
	// are used.
	//
	// For example, in order to automatically look up compiled Go protos that
	// have been imported and be able to use them as imports, set this to
	// desc.LoadFileDescriptor.
	LookupImport func(string) (*desc.FileDescriptor, error)

	// LookupImportProto has the same functionality as LookupImport, however it returns
	// a FileDescriptorProto instead of a FileDescriptor.
	//
	// It is an error to set both LookupImport and LookupImportProto.
	LookupImportProto func(string) (*dpb.FileDescriptorProto, error)

	// Used to create a reader for a given filename, when loading proto source
	// file contents. If unset, os.Open is used. If ImportPaths is also empty
	// then relative paths are will be relative to the process's current working
	// directory.
	Accessor FileAccessor

	// If true, the resulting file descriptors will retain source code info,
	// that maps elements to their location in the source files as well as
	// includes comments found during parsing (and attributed to elements of
	// the source file).
	IncludeSourceCodeInfo bool

	// If true, the results from ParseFilesButDoNotLink will be passed through
	// some additional validations. But only constraints that do not require
	// linking can be checked. These include proto2 vs. proto3 language features,
	// looking for incorrect usage of reserved names or tags, and ensuring that
	// fields have unique tags and that enum values have unique numbers (unless
	// the enum allows aliases).
	ValidateUnlinkedFiles bool

	// If true, the results from ParseFilesButDoNotLink will have options
	// interpreted. Any uninterpretable options (including any custom options or
	// options that refer to message and enum types, which can only be
	// interpreted after linking) will be left in uninterpreted_options. Also,
	// the "default" pseudo-option for fields can only be interpreted for scalar
	// fields, excluding enums. (Interpreting default values for enum fields
	// requires resolving enum names, which requires linking.)
	InterpretOptionsInUnlinkedFiles bool

	// A custom reporter of syntax and link errors. If not specified, the
	// default reporter just returns the reported error, which causes parsing
	// to abort after encountering a single error.
	//
	// The reporter is not invoked for system or I/O errors, only for syntax and
	// link errors.
	ErrorReporter ErrorReporter

	// A custom reporter of warnings. If not specified, warning messages are ignored.
	WarningReporter WarningReporter
}

// ParseFiles parses the named files into descriptors. The returned slice has
// the same number of entries as the give filenames, in the same order. So the
// first returned descriptor corresponds to the first given name, and so on.
//
// All dependencies for all specified files (including transitive dependencies)
// must be accessible via the parser's Accessor or a link error will occur. The
// exception to this rule is that files can import standard Google-provided
// files -- e.g. google/protobuf/*.proto -- without needing to supply sources
// for these files. Like protoc, this parser has a built-in version of these
// files it can use if they aren't explicitly supplied.
//
// If the Parser has no ErrorReporter set and a syntax or link error occurs,
// parsing will abort with the first such error encountered. If there is an
// ErrorReporter configured and it returns non-nil, parsing will abort with the
// error it returns. If syntax or link errors are encountered but the configured
// ErrorReporter always returns nil, the parse fails with ErrInvalidSource.
func (p Parser) ParseFiles(filenames ...string) ([]*desc.FileDescriptor, error) {
	accessor := p.Accessor
	if accessor == nil {
		accessor = func(name string) (io.ReadCloser, error) {
			return os.Open(name)
		}
	}
	paths := p.ImportPaths
	if len(paths) > 0 {
		acc := accessor
		accessor = func(name string) (io.ReadCloser, error) {
			var ret error
			for _, path := range paths {
				f, err := acc(filepath.Join(path, name))
				if err != nil {
					if ret == nil {
						ret = err
					}
					continue
				}
				return f, nil
			}
			return nil, ret
		}
	}
	lookupImport, err := p.getLookupImport()
	if err != nil {
		return nil, err
	}

	protos := map[string]*parseResult{}
	results := &parseResults{
		resultsByFilename:      protos,
		recursive:              true,
		validate:               true,
		createDescriptorProtos: true,
	}
	errs := newErrorHandler(p.ErrorReporter, p.WarningReporter)
	parseProtoFiles(accessor, filenames, errs, results, lookupImport)
	if err := errs.getError(); err != nil {
		return nil, err
	}
	if p.InferImportPaths {
		// TODO: if this re-writes one of the names in filenames, lookups below will break
		protos = fixupFilenames(protos)
	}
	l := newLinker(results, errs)
	linkedProtos, err := l.linkFiles()
	if err != nil {
		return nil, err
	}
	// Now we're done linking, so we can check to see if any imports were unused
	for _, file := range filenames {
		l.checkForUnusedImports(file)
	}
	if p.IncludeSourceCodeInfo {
		for name, fd := range linkedProtos {
			pr := protos[name]
			fd.AsFileDescriptorProto().SourceCodeInfo = pr.generateSourceCodeInfo()
			internal.RecomputeSourceInfo(fd)
		}
	}
	fds := make([]*desc.FileDescriptor, len(filenames))
	for i, name := range filenames {
		fd := linkedProtos[name]
		fds[i] = fd
	}
	return fds, nil
}

// ParseFilesButDoNotLink parses the named files into descriptor protos. The
// results are just protos, not fully-linked descriptors. It is possible that
// descriptors are invalid and still be returned in parsed form without error
// due to the fact that the linking step is skipped (and thus many validation
// steps omitted).
//
// There are a few side effects to not linking the descriptors:
//   1. No options will be interpreted. Options can refer to extensions or have
//      message and enum types. Without linking, these extension and type
//      references are not resolved, so the options may not be interpretable.
//      So all options will appear in UninterpretedOption fields of the various
//      descriptor options messages.
//   2. Type references will not be resolved. This means that the actual type
//      names in the descriptors may be unqualified and even relative to the
//      scope in which the type reference appears. This goes for fields that
//      have message and enum types. It also applies to methods and their
//      references to request and response message types.
//   3. Type references are not known. For non-scalar fields, until the type
//      name is resolved (during linking), it is not known whether the type
//      refers to a message or an enum. So all fields with such type references
//      will not have their Type set, only the TypeName.
//
// This method will still validate the syntax of parsed files. If the parser's
// ValidateUnlinkedFiles field is true, additional checks, beyond syntax will
// also be performed.
//
// If the Parser has no ErrorReporter set and a syntax error occurs, parsing
// will abort with the first such error encountered. If there is an
// ErrorReporter configured and it returns non-nil, parsing will abort with the
// error it returns. If syntax errors are encountered but the configured
// ErrorReporter always returns nil, the parse fails with ErrInvalidSource.
func (p Parser) ParseFilesButDoNotLink(filenames ...string) ([]*dpb.FileDescriptorProto, error) {
	accessor := p.Accessor
	if accessor == nil {
		accessor = func(name string) (io.ReadCloser, error) {
			return os.Open(name)
		}
	}
	lookupImport, err := p.getLookupImport()
	if err != nil {
		return nil, err
	}

	protos := map[string]*parseResult{}
	errs := newErrorHandler(p.ErrorReporter, p.WarningReporter)
	results := &parseResults{
		resultsByFilename:      protos,
		validate:               p.ValidateUnlinkedFiles,
		createDescriptorProtos: true,
	}
	parseProtoFiles(accessor, filenames, errs, results, lookupImport)
	if err := errs.getError(); err != nil {
		return nil, err
	}
	if p.InferImportPaths {
		// TODO: if this re-writes one of the names in filenames, lookups below will break
		protos = fixupFilenames(protos)
	}
	fds := make([]*dpb.FileDescriptorProto, len(filenames))
	for i, name := range filenames {
		pr := protos[name]
		fd := pr.fd
		if p.InterpretOptionsInUnlinkedFiles {
			// parsing options will be best effort
			pr.lenient = true
			// we don't want the real error reporter see any errors
			pr.errs.errReporter = func(err ErrorWithPos) error {
				return err
			}
			_ = interpretFileOptions(nil, pr, poorFileDescriptorish{FileDescriptorProto: fd})
		}
		if p.IncludeSourceCodeInfo {
			fd.SourceCodeInfo = pr.generateSourceCodeInfo()
		}
		fds[i] = fd
	}
	return fds, nil
}

// ParseToAST parses the named files into ASTs, or Abstract Syntax Trees. This
// is for consumers of proto files that don't care about compiling the files to
// descriptors, but care deeply about a non-lossy structured representation of
// the source (since descriptors are lossy). This includes formatting tools and
// possibly linters, too.
//
// If the requested filenames include standard imports (such as
// "google/protobuf/empty.proto") and no source is provided, the corresponding
// AST in the returned slice will be nil. These standard imports are only
// available for use as descriptors; no source is available unless it is
// provided by the configured Accessor.
//
// If the Parser has no ErrorReporter set and a syntax error occurs, parsing
// will abort with the first such error encountered. If there is an
// ErrorReporter configured and it returns non-nil, parsing will abort with the
// error it returns. If syntax errors are encountered but the configured
// ErrorReporter always returns nil, the parse fails with ErrInvalidSource.
func (p Parser) ParseToAST(filenames ...string) ([]*ast.FileNode, error) {
	accessor := p.Accessor
	if accessor == nil {
		accessor = func(name string) (io.ReadCloser, error) {
			return os.Open(name)
		}
	}
	lookupImport, err := p.getLookupImport()
	if err != nil {
		return nil, err
	}

	protos := map[string]*parseResult{}
	errs := newErrorHandler(p.ErrorReporter, p.WarningReporter)
	parseProtoFiles(accessor, filenames, errs, &parseResults{resultsByFilename: protos}, lookupImport)
	if err := errs.getError(); err != nil {
		return nil, err
	}
	ret := make([]*ast.FileNode, 0, len(filenames))
	for _, name := range filenames {
		ret = append(ret, protos[name].root)
	}
	return ret, nil
}

func (p Parser) getLookupImport() (func(string) (*dpb.FileDescriptorProto, error), error) {
	if p.LookupImport != nil && p.LookupImportProto != nil {
		return nil, ErrLookupImportAndProtoSet
	}
	if p.LookupImportProto != nil {
		return p.LookupImportProto, nil
	}
	if p.LookupImport != nil {
		return func(path string) (*dpb.FileDescriptorProto, error) {
			value, err := p.LookupImport(path)
			if value != nil {
				return value.AsFileDescriptorProto(), err
			}
			return nil, err
		}, nil
	}
	return nil, nil
}

func fixupFilenames(protos map[string]*parseResult) map[string]*parseResult {
	// In the event that the given filenames (keys in the supplied map) do not
	// match the actual paths used in 'import' statements in the files, we try
	// to revise names in the protos so that they will match and be linkable.
	revisedProtos := map[string]*parseResult{}

	protoPaths := map[string]struct{}{}
	// TODO: this is O(n^2) but could likely be O(n) with a clever data structure (prefix tree that is indexed backwards?)
	importCandidates := map[string]map[string]struct{}{}
	candidatesAvailable := map[string]struct{}{}
	for name := range protos {
		candidatesAvailable[name] = struct{}{}
		for _, f := range protos {
			for _, imp := range f.fd.Dependency {
				if strings.HasSuffix(name, imp) {
					candidates := importCandidates[imp]
					if candidates == nil {
						candidates = map[string]struct{}{}
						importCandidates[imp] = candidates
					}
					candidates[name] = struct{}{}
				}
			}
		}
	}
	for imp, candidates := range importCandidates {
		// if we found multiple possible candidates, use the one that is an exact match
		// if it exists, and otherwise, guess that it's the shortest path (fewest elements)
		var best string
		for c := range candidates {
			if _, ok := candidatesAvailable[c]; !ok {
				// already used this candidate and re-written its filename accordingly
				continue
			}
			if c == imp {
				// exact match!
				best = c
				break
			}
			if best == "" {
				best = c
			} else {
				// HACK: we can't actually tell which files is supposed to match
				// this import, so arbitrarily pick the "shorter" one (fewest
				// path elements) or, on a tie, the lexically earlier one
				minLen := strings.Count(best, string(filepath.Separator))
				cLen := strings.Count(c, string(filepath.Separator))
				if cLen < minLen || (cLen == minLen && c < best) {
					best = c
				}
			}
		}
		if best != "" {
			prefix := best[:len(best)-len(imp)]
			if len(prefix) > 0 {
				protoPaths[prefix] = struct{}{}
			}
			f := protos[best]
			f.fd.Name = proto.String(imp)
			revisedProtos[imp] = f
			delete(candidatesAvailable, best)
		}
	}

	if len(candidatesAvailable) == 0 {
		return revisedProtos
	}

	if len(protoPaths) == 0 {
		for c := range candidatesAvailable {
			revisedProtos[c] = protos[c]
		}
		return revisedProtos
	}

	// Any remaining candidates are entry-points (not imported by others), so
	// the best bet to "fixing" their file name is to see if they're in one of
	// the proto paths we found, and if so strip that prefix.
	protoPathStrs := make([]string, len(protoPaths))
	i := 0
	for p := range protoPaths {
		protoPathStrs[i] = p
		i++
	}
	sort.Strings(protoPathStrs)
	// we look at paths in reverse order, so we'll use a longer proto path if
	// there is more than one match
	for c := range candidatesAvailable {
		var imp string
		for i := len(protoPathStrs) - 1; i >= 0; i-- {
			p := protoPathStrs[i]
			if strings.HasPrefix(c, p) {
				imp = c[len(p):]
				break
			}
		}
		if imp != "" {
			f := protos[c]
			f.fd.Name = proto.String(imp)
			revisedProtos[imp] = f
		} else {
			revisedProtos[c] = protos[c]
		}
	}

	return revisedProtos
}

func parseProtoFiles(acc FileAccessor, filenames []string, errs *errorHandler, parsed *parseResults, lookupImport func(string) (*dpb.FileDescriptorProto, error)) {
	for _, name := range filenames {
		parseProtoFile(acc, name, nil, errs, parsed, lookupImport)
		if errs.err != nil {
			return
		}
	}
}

func parseProtoFile(acc FileAccessor, filename string, importLoc *SourcePos, errs *errorHandler, results *parseResults, lookupImport func(string) (*dpb.FileDescriptorProto, error)) {
	if results.has(filename) {
		return
	}
	if lookupImport == nil {
		lookupImport = func(string) (*dpb.FileDescriptorProto, error) {
			return nil, errors.New("no import lookup function")
		}
	}
	in, err := acc(filename)
	var result *parseResult
	if err == nil {
		// try to parse the bytes accessed
		func() {
			defer func() {
				// if we've already parsed contents, an error
				// closing need not fail this operation
				_ = in.Close()
			}()
			result = parseProto(filename, in, errs, results.validate, results.createDescriptorProtos)
		}()
	} else if d, lookupErr := lookupImport(filename); lookupErr == nil {
		// This is a user-provided descriptor, which is acting similarly to a
		// well-known import.
		result = &parseResult{fd: proto.Clone(d).(*dpb.FileDescriptorProto)}
	} else if d, ok := standardImports[filename]; ok {
		// it's a well-known import
		// (we clone it to make sure we're not sharing state with other
		//  parsers, which could result in unsafe races if multiple
		//  parsers are trying to access it concurrently)
		result = &parseResult{fd: proto.Clone(d).(*dpb.FileDescriptorProto)}
	} else {
		if !strings.Contains(err.Error(), filename) {
			// an error message that doesn't indicate the file is awful!
			// this cannot be %w as this is not compatible with go <= 1.13
			err = errorWithFilename{
				underlying: err,
				filename:   filename,
			}
		}
		// The top-level loop in parseProtoFiles calls this with nil for the top-level files
		// importLoc is only for imports, otherwise we do not want to return a ErrorWithSourcePos
		// ErrorWithSourcePos should always have a non-nil SourcePos
		if importLoc != nil {
			// associate the error with the import line
			err = ErrorWithSourcePos{
				Pos:        importLoc,
				Underlying: err,
			}
		}
		_ = errs.handleError(err)
		return
	}

	results.add(filename, result)

	if errs.err != nil {
		return // abort
	}

	if results.recursive {
		fd := result.fd
		decl := result.getFileNode(fd)
		fnode, ok := decl.(*ast.FileNode)
		if !ok {
			// no AST for this file? use imports in descriptor
			for _, dep := range fd.Dependency {
				parseProtoFile(acc, dep, decl.Start(), errs, results, lookupImport)
				if errs.getError() != nil {
					return // abort
				}
			}
			return
		}
		// we have an AST; use it so we can report import location in errors
		for _, decl := range fnode.Decls {
			if dep, ok := decl.(*ast.ImportNode); ok {
				parseProtoFile(acc, dep.Name.AsString(), dep.Name.Start(), errs, results, lookupImport)
				if errs.getError() != nil {
					return // abort
				}
			}
		}
	}
}

type parseResults struct {
	resultsByFilename map[string]*parseResult
	filenames         []string

	recursive, validate, createDescriptorProtos bool
}

func (r *parseResults) has(filename string) bool {
	_, ok := r.resultsByFilename[filename]
	return ok
}

func (r *parseResults) add(filename string, result *parseResult) {
	r.resultsByFilename[filename] = result
	r.filenames = append(r.filenames, filename)
}

type parseResult struct {
	// handles any errors encountered during parsing, construction of file descriptor,
	// or validation
	errs *errorHandler

	// the root of the AST
	root *ast.FileNode
	// the parsed file descriptor
	fd *dpb.FileDescriptorProto

	// if set to true, enables lenient interpretation of options, where
	// unrecognized options will be left uninterpreted instead of resulting in a
	// link error
	lenient bool

	// a map of elements in the descriptor to nodes in the AST
	// (for extracting position information when validating the descriptor)
	nodes map[proto.Message]ast.Node

	// a map of uninterpreted option AST nodes to their relative path
	// in the resulting options message
	interpretedOptions map[*ast.OptionNode][]int32
}

func (r *parseResult) getFileNode(f *dpb.FileDescriptorProto) ast.FileDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(f.GetName())
	}
	return r.nodes[f].(ast.FileDeclNode)
}

func (r *parseResult) getOptionNode(o *dpb.UninterpretedOption) ast.OptionDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[o].(ast.OptionDeclNode)
}

func (r *parseResult) getOptionNamePartNode(o *dpb.UninterpretedOption_NamePart) ast.Node {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[o]
}

func (r *parseResult) getFieldNode(f *dpb.FieldDescriptorProto) ast.FieldDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[f].(ast.FieldDeclNode)
}

func (r *parseResult) getExtensionRangeNode(e *dpb.DescriptorProto_ExtensionRange) ast.RangeDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[e].(ast.RangeDeclNode)
}

func (r *parseResult) getMessageReservedRangeNode(rr *dpb.DescriptorProto_ReservedRange) ast.RangeDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[rr].(ast.RangeDeclNode)
}

func (r *parseResult) getEnumNode(e *dpb.EnumDescriptorProto) ast.Node {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[e]
}

func (r *parseResult) getEnumValueNode(e *dpb.EnumValueDescriptorProto) ast.EnumValueDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[e].(ast.EnumValueDeclNode)
}

func (r *parseResult) getEnumReservedRangeNode(rr *dpb.EnumDescriptorProto_EnumReservedRange) ast.RangeDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[rr].(ast.RangeDeclNode)
}

func (r *parseResult) getMethodNode(m *dpb.MethodDescriptorProto) ast.RPCDeclNode {
	if r.nodes == nil {
		return ast.NewNoSourceNode(r.fd.GetName())
	}
	return r.nodes[m].(ast.RPCDeclNode)
}

func (r *parseResult) putFileNode(f *dpb.FileDescriptorProto, n *ast.FileNode) {
	r.nodes[f] = n
}

func (r *parseResult) putOptionNode(o *dpb.UninterpretedOption, n *ast.OptionNode) {
	r.nodes[o] = n
}

func (r *parseResult) putOptionNamePartNode(o *dpb.UninterpretedOption_NamePart, n *ast.FieldReferenceNode) {
	r.nodes[o] = n
}

func (r *parseResult) putMessageNode(m *dpb.DescriptorProto, n ast.MessageDeclNode) {
	r.nodes[m] = n
}

func (r *parseResult) putFieldNode(f *dpb.FieldDescriptorProto, n ast.FieldDeclNode) {
	r.nodes[f] = n
}

func (r *parseResult) putOneOfNode(o *dpb.OneofDescriptorProto, n *ast.OneOfNode) {
	r.nodes[o] = n
}

func (r *parseResult) putExtensionRangeNode(e *dpb.DescriptorProto_ExtensionRange, n *ast.RangeNode) {
	r.nodes[e] = n
}

func (r *parseResult) putMessageReservedRangeNode(rr *dpb.DescriptorProto_ReservedRange, n *ast.RangeNode) {
	r.nodes[rr] = n
}

func (r *parseResult) putEnumNode(e *dpb.EnumDescriptorProto, n *ast.EnumNode) {
	r.nodes[e] = n
}

func (r *parseResult) putEnumValueNode(e *dpb.EnumValueDescriptorProto, n *ast.EnumValueNode) {
	r.nodes[e] = n
}

func (r *parseResult) putEnumReservedRangeNode(rr *dpb.EnumDescriptorProto_EnumReservedRange, n *ast.RangeNode) {
	r.nodes[rr] = n
}

func (r *parseResult) putServiceNode(s *dpb.ServiceDescriptorProto, n *ast.ServiceNode) {
	r.nodes[s] = n
}

func (r *parseResult) putMethodNode(m *dpb.MethodDescriptorProto, n *ast.RPCNode) {
	r.nodes[m] = n
}

func parseProto(filename string, r io.Reader, errs *errorHandler, validate, createProtos bool) *parseResult {
	beforeErrs := errs.errsReported
	lx := newLexer(r, filename, errs)
	protoParse(lx)
	if lx.res == nil || len(lx.res.Children()) == 0 {
		// nil AST means there was an error that prevented any parsing
		// or the file was empty; synthesize empty non-nil AST
		lx.res = ast.NewEmptyFileNode(filename)
	}
	if lx.eof != nil {
		lx.res.FinalComments = lx.eof.LeadingComments()
		lx.res.FinalWhitespace = lx.eof.LeadingWhitespace()
	}
	res := createParseResult(filename, lx.res, errs, createProtos)
	if validate && errs.err == nil {
		validateBasic(res, errs.errsReported > beforeErrs)
	}

	return res
}

func createParseResult(filename string, file *ast.FileNode, errs *errorHandler, createProtos bool) *parseResult {
	res := &parseResult{
		errs:               errs,
		root:               file,
		nodes:              map[proto.Message]ast.Node{},
		interpretedOptions: map[*ast.OptionNode][]int32{},
	}
	if createProtos {
		res.createFileDescriptor(filename, file)
	}
	return res
}

func checkTag(pos *SourcePos, v uint64, maxTag int32) error {
	if v < 1 {
		return errorWithPos(pos, "tag number %d must be greater than zero", v)
	} else if v > uint64(maxTag) {
		return errorWithPos(pos, "tag number %d is higher than max allowed tag number (%d)", v, maxTag)
	} else if v >= internal.SpecialReservedStart && v <= internal.SpecialReservedEnd {
		return errorWithPos(pos, "tag number %d is in disallowed reserved range %d-%d", v, internal.SpecialReservedStart, internal.SpecialReservedEnd)
	}
	return nil
}

func checkExtensionsInFile(fd *desc.FileDescriptor, res *parseResult) error {
	for _, fld := range fd.GetExtensions() {
		if err := checkExtension(fld, res); err != nil {
			return err
		}
	}
	for _, md := range fd.GetMessageTypes() {
		if err := checkExtensionsInMessage(md, res); err != nil {
			return err
		}
	}
	return nil
}

func checkExtensionsInMessage(md *desc.MessageDescriptor, res *parseResult) error {
	for _, fld := range md.GetNestedExtensions() {
		if err := checkExtension(fld, res); err != nil {
			return err
		}
	}
	for _, nmd := range md.GetNestedMessageTypes() {
		if err := checkExtensionsInMessage(nmd, res); err != nil {
			return err
		}
	}
	return nil
}

func checkExtension(fld *desc.FieldDescriptor, res *parseResult) error {
	// NB: It's a little gross that we don't enforce these in validateBasic().
	// But requires some minimal linking to resolve the extendee, so we can
	// interrogate its descriptor.
	if fld.GetOwner().GetMessageOptions().GetMessageSetWireFormat() {
		// Message set wire format requires that all extensions be messages
		// themselves (no scalar extensions)
		if fld.GetType() != dpb.FieldDescriptorProto_TYPE_MESSAGE {
			pos := res.getFieldNode(fld.AsFieldDescriptorProto()).FieldType().Start()
			return errorWithPos(pos, "messages with message-set wire format cannot contain scalar extensions, only messages")
		}
	} else {
		// In validateBasic() we just made sure these were within bounds for any message. But
		// now that things are linked, we can check if the extendee is messageset wire format
		// and, if not, enforce tighter limit.
		if fld.GetNumber() > internal.MaxNormalTag {
			pos := res.getFieldNode(fld.AsFieldDescriptorProto()).FieldTag().Start()
			return errorWithPos(pos, "tag number %d is higher than max allowed tag number (%d)", fld.GetNumber(), internal.MaxNormalTag)
		}
	}

	return nil
}

func aggToString(agg []*ast.MessageFieldNode, buf *bytes.Buffer) {
	buf.WriteString("{")
	for _, a := range agg {
		buf.WriteString(" ")
		buf.WriteString(a.Name.Value())
		if v, ok := a.Val.(*ast.MessageLiteralNode); ok {
			aggToString(v.Elements, buf)
		} else {
			buf.WriteString(": ")
			elementToString(a.Val.Value(), buf)
		}
	}
	buf.WriteString(" }")
}

func elementToString(v interface{}, buf *bytes.Buffer) {
	switch v := v.(type) {
	case bool, int64, uint64, ast.Identifier:
		_, _ = fmt.Fprintf(buf, "%v", v)
	case float64:
		if math.IsInf(v, 1) {
			buf.WriteString(": inf")
		} else if math.IsInf(v, -1) {
			buf.WriteString(": -inf")
		} else if math.IsNaN(v) {
			buf.WriteString(": nan")
		} else {
			_, _ = fmt.Fprintf(buf, ": %v", v)
		}
	case string:
		buf.WriteRune('"')
		writeEscapedBytes(buf, []byte(v))
		buf.WriteRune('"')
	case []ast.ValueNode:
		buf.WriteString(": [")
		first := true
		for _, e := range v {
			if first {
				first = false
			} else {
				buf.WriteString(", ")
			}
			elementToString(e.Value(), buf)
		}
		buf.WriteString("]")
	case []*ast.MessageFieldNode:
		aggToString(v, buf)
	}
}

func writeEscapedBytes(buf *bytes.Buffer, b []byte) {
	for _, c := range b {
		switch c {
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		case '"':
			buf.WriteString("\\\"")
		case '\'':
			buf.WriteString("\\'")
		case '\\':
			buf.WriteString("\\\\")
		default:
			if c >= 0x20 && c <= 0x7f && c != '"' && c != '\\' {
				// simple printable characters
				buf.WriteByte(c)
			} else {
				// use octal escape for all other values
				buf.WriteRune('\\')
				buf.WriteByte('0' + ((c >> 6) & 0x7))
				buf.WriteByte('0' + ((c >> 3) & 0x7))
				buf.WriteByte('0' + (c & 0x7))
			}
		}
	}
}
