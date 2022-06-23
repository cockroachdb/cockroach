package desc

import (
	"errors"
	"fmt"
	"strings"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/desc/internal"
	intn "github.com/jhump/protoreflect/internal"
)

// CreateFileDescriptor instantiates a new file descriptor for the given descriptor proto.
// The file's direct dependencies must be provided. If the given dependencies do not include
// all of the file's dependencies or if the contents of the descriptors are internally
// inconsistent (e.g. contain unresolvable symbols) then an error is returned.
func CreateFileDescriptor(fd *dpb.FileDescriptorProto, deps ...*FileDescriptor) (*FileDescriptor, error) {
	return createFileDescriptor(fd, deps, nil)
}

func createFileDescriptor(fd *dpb.FileDescriptorProto, deps []*FileDescriptor, r *ImportResolver) (*FileDescriptor, error) {
	ret := &FileDescriptor{
		proto:      fd,
		symbols:    map[string]Descriptor{},
		fieldIndex: map[string]map[int32]*FieldDescriptor{},
	}
	pkg := fd.GetPackage()

	// populate references to file descriptor dependencies
	files := map[string]*FileDescriptor{}
	for _, f := range deps {
		files[f.proto.GetName()] = f
	}
	ret.deps = make([]*FileDescriptor, len(fd.GetDependency()))
	for i, d := range fd.GetDependency() {
		resolved := r.ResolveImport(fd.GetName(), d)
		ret.deps[i] = files[resolved]
		if ret.deps[i] == nil {
			if resolved != d {
				ret.deps[i] = files[d]
			}
			if ret.deps[i] == nil {
				return nil, intn.ErrNoSuchFile(d)
			}
		}
	}
	ret.publicDeps = make([]*FileDescriptor, len(fd.GetPublicDependency()))
	for i, pd := range fd.GetPublicDependency() {
		ret.publicDeps[i] = ret.deps[pd]
	}
	ret.weakDeps = make([]*FileDescriptor, len(fd.GetWeakDependency()))
	for i, wd := range fd.GetWeakDependency() {
		ret.weakDeps[i] = ret.deps[wd]
	}
	ret.isProto3 = fd.GetSyntax() == "proto3"

	// populate all tables of child descriptors
	for _, m := range fd.GetMessageType() {
		md, n := createMessageDescriptor(ret, ret, pkg, m, ret.symbols)
		ret.symbols[n] = md
		ret.messages = append(ret.messages, md)
	}
	for _, e := range fd.GetEnumType() {
		ed, n := createEnumDescriptor(ret, ret, pkg, e, ret.symbols)
		ret.symbols[n] = ed
		ret.enums = append(ret.enums, ed)
	}
	for _, ex := range fd.GetExtension() {
		exd, n := createFieldDescriptor(ret, ret, pkg, ex)
		ret.symbols[n] = exd
		ret.extensions = append(ret.extensions, exd)
	}
	for _, s := range fd.GetService() {
		sd, n := createServiceDescriptor(ret, pkg, s, ret.symbols)
		ret.symbols[n] = sd
		ret.services = append(ret.services, sd)
	}

	ret.sourceInfo = internal.CreateSourceInfoMap(fd)
	ret.sourceInfoRecomputeFunc = ret.recomputeSourceInfo

	// now we can resolve all type references and source code info
	scopes := []scope{fileScope(ret)}
	path := make([]int32, 1, 8)
	path[0] = internal.File_messagesTag
	for i, md := range ret.messages {
		if err := md.resolve(append(path, int32(i)), scopes); err != nil {
			return nil, err
		}
	}
	path[0] = internal.File_enumsTag
	for i, ed := range ret.enums {
		ed.resolve(append(path, int32(i)))
	}
	path[0] = internal.File_extensionsTag
	for i, exd := range ret.extensions {
		if err := exd.resolve(append(path, int32(i)), scopes); err != nil {
			return nil, err
		}
	}
	path[0] = internal.File_servicesTag
	for i, sd := range ret.services {
		if err := sd.resolve(append(path, int32(i)), scopes); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// CreateFileDescriptors constructs a set of descriptors, one for each of the
// given descriptor protos. The given set of descriptor protos must include all
// transitive dependencies for every file.
func CreateFileDescriptors(fds []*dpb.FileDescriptorProto) (map[string]*FileDescriptor, error) {
	return createFileDescriptors(fds, nil)
}

func createFileDescriptors(fds []*dpb.FileDescriptorProto, r *ImportResolver) (map[string]*FileDescriptor, error) {
	if len(fds) == 0 {
		return nil, nil
	}
	files := map[string]*dpb.FileDescriptorProto{}
	resolved := map[string]*FileDescriptor{}
	var name string
	for _, fd := range fds {
		name = fd.GetName()
		files[name] = fd
	}
	for _, fd := range fds {
		_, err := createFromSet(fd.GetName(), r, nil, files, resolved)
		if err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

// ToFileDescriptorSet creates a FileDescriptorSet proto that contains all of the given
// file descriptors and their transitive dependencies. The files are topologically sorted
// so that a file will always appear after its dependencies.
func ToFileDescriptorSet(fds ...*FileDescriptor) *dpb.FileDescriptorSet {
	var fdps []*dpb.FileDescriptorProto
	addAllFiles(fds, &fdps, map[string]struct{}{})
	return &dpb.FileDescriptorSet{File: fdps}
}

func addAllFiles(src []*FileDescriptor, results *[]*dpb.FileDescriptorProto, seen map[string]struct{}) {
	for _, fd := range src {
		if _, ok := seen[fd.GetName()]; ok {
			continue
		}
		seen[fd.GetName()] = struct{}{}
		addAllFiles(fd.GetDependencies(), results, seen)
		*results = append(*results, fd.AsFileDescriptorProto())
	}
}

// CreateFileDescriptorFromSet creates a descriptor from the given file descriptor set. The
// set's *last* file will be the returned descriptor. The set's remaining files must comprise
// the full set of transitive dependencies of that last file. This is the same format and
// order used by protoc when emitting a FileDescriptorSet file with an invocation like so:
//    protoc --descriptor_set_out=./test.protoset --include_imports -I. test.proto
func CreateFileDescriptorFromSet(fds *dpb.FileDescriptorSet) (*FileDescriptor, error) {
	return createFileDescriptorFromSet(fds, nil)
}

func createFileDescriptorFromSet(fds *dpb.FileDescriptorSet, r *ImportResolver) (*FileDescriptor, error) {
	result, err := createFileDescriptorsFromSet(fds, r)
	if err != nil {
		return nil, err
	}
	files := fds.GetFile()
	lastFilename := files[len(files)-1].GetName()
	return result[lastFilename], nil
}

// CreateFileDescriptorsFromSet creates file descriptors from the given file descriptor set.
// The returned map includes all files in the set, keyed b name. The set must include the
// full set of transitive dependencies for all files therein or else a link error will occur
// and be returned instead of the slice of descriptors. This is the same format used by
// protoc when a FileDescriptorSet file with an invocation like so:
//    protoc --descriptor_set_out=./test.protoset --include_imports -I. test.proto
func CreateFileDescriptorsFromSet(fds *dpb.FileDescriptorSet) (map[string]*FileDescriptor, error) {
	return createFileDescriptorsFromSet(fds, nil)
}

func createFileDescriptorsFromSet(fds *dpb.FileDescriptorSet, r *ImportResolver) (map[string]*FileDescriptor, error) {
	files := fds.GetFile()
	if len(files) == 0 {
		return nil, errors.New("file descriptor set is empty")
	}
	return createFileDescriptors(files, r)
}

// createFromSet creates a descriptor for the given filename. It recursively
// creates descriptors for the given file's dependencies.
func createFromSet(filename string, r *ImportResolver, seen []string, files map[string]*dpb.FileDescriptorProto, resolved map[string]*FileDescriptor) (*FileDescriptor, error) {
	for _, s := range seen {
		if filename == s {
			return nil, fmt.Errorf("cycle in imports: %s", strings.Join(append(seen, filename), " -> "))
		}
	}
	seen = append(seen, filename)

	if d, ok := resolved[filename]; ok {
		return d, nil
	}
	fdp := files[filename]
	if fdp == nil {
		return nil, intn.ErrNoSuchFile(filename)
	}
	deps := make([]*FileDescriptor, len(fdp.GetDependency()))
	for i, depName := range fdp.GetDependency() {
		resolvedDep := r.ResolveImport(filename, depName)
		dep, err := createFromSet(resolvedDep, r, seen, files, resolved)
		if _, ok := err.(intn.ErrNoSuchFile); ok && resolvedDep != depName {
			dep, err = createFromSet(depName, r, seen, files, resolved)
		}
		if err != nil {
			return nil, err
		}
		deps[i] = dep
	}
	d, err := createFileDescriptor(fdp, deps, r)
	if err != nil {
		return nil, err
	}
	resolved[filename] = d
	return d, nil
}
