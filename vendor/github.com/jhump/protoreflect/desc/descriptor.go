package desc

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/desc/internal"
)

// Descriptor is the common interface implemented by all descriptor objects.
type Descriptor interface {
	// GetName returns the name of the object described by the descriptor. This will
	// be a base name that does not include enclosing message names or the package name.
	// For file descriptors, this indicates the path and name to the described file.
	GetName() string
	// GetFullyQualifiedName returns the fully-qualified name of the object described by
	// the descriptor. This will include the package name and any enclosing message names.
	// For file descriptors, this returns the path and name to the described file (same as
	// GetName).
	GetFullyQualifiedName() string
	// GetParent returns the enclosing element in a proto source file. If the described
	// object is a top-level object, this returns the file descriptor. Otherwise, it returns
	// the element in which the described object was declared. File descriptors have no
	// parent and return nil.
	GetParent() Descriptor
	// GetFile returns the file descriptor in which this element was declared. File
	// descriptors return themselves.
	GetFile() *FileDescriptor
	// GetOptions returns the options proto containing options for the described element.
	GetOptions() proto.Message
	// GetSourceInfo returns any source code information that was present in the file
	// descriptor. Source code info is optional. If no source code info is available for
	// the element (including if there is none at all in the file descriptor) then this
	// returns nil
	GetSourceInfo() *dpb.SourceCodeInfo_Location
	// AsProto returns the underlying descriptor proto for this descriptor.
	AsProto() proto.Message
}

type sourceInfoRecomputeFunc = internal.SourceInfoComputeFunc

// FileDescriptor describes a proto source file.
type FileDescriptor struct {
	proto      *dpb.FileDescriptorProto
	symbols    map[string]Descriptor
	deps       []*FileDescriptor
	publicDeps []*FileDescriptor
	weakDeps   []*FileDescriptor
	messages   []*MessageDescriptor
	enums      []*EnumDescriptor
	extensions []*FieldDescriptor
	services   []*ServiceDescriptor
	fieldIndex map[string]map[int32]*FieldDescriptor
	isProto3   bool
	sourceInfo internal.SourceInfoMap
	sourceInfoRecomputeFunc
}

func (fd *FileDescriptor) recomputeSourceInfo() {
	internal.PopulateSourceInfoMap(fd.proto, fd.sourceInfo)
}

func (fd *FileDescriptor) registerField(field *FieldDescriptor) {
	fields := fd.fieldIndex[field.owner.GetFullyQualifiedName()]
	if fields == nil {
		fields = map[int32]*FieldDescriptor{}
		fd.fieldIndex[field.owner.GetFullyQualifiedName()] = fields
	}
	fields[field.GetNumber()] = field
}

// GetName returns the name of the file, as it was given to the protoc invocation
// to compile it, possibly including path (relative to a directory in the proto
// import path).
func (fd *FileDescriptor) GetName() string {
	return fd.proto.GetName()
}

// GetFullyQualifiedName returns the name of the file, same as GetName. It is
// present to satisfy the Descriptor interface.
func (fd *FileDescriptor) GetFullyQualifiedName() string {
	return fd.proto.GetName()
}

// GetPackage returns the name of the package declared in the file.
func (fd *FileDescriptor) GetPackage() string {
	return fd.proto.GetPackage()
}

// GetParent always returns nil: files are the root of descriptor hierarchies.
// Is it present to satisfy the Descriptor interface.
func (fd *FileDescriptor) GetParent() Descriptor {
	return nil
}

// GetFile returns the receiver, which is a file descriptor. This is present
// to satisfy the Descriptor interface.
func (fd *FileDescriptor) GetFile() *FileDescriptor {
	return fd
}

// GetOptions returns the file's options. Most usages will be more interested
// in GetFileOptions, which has a concrete return type. This generic version
// is present to satisfy the Descriptor interface.
func (fd *FileDescriptor) GetOptions() proto.Message {
	return fd.proto.GetOptions()
}

// GetFileOptions returns the file's options.
func (fd *FileDescriptor) GetFileOptions() *dpb.FileOptions {
	return fd.proto.GetOptions()
}

// GetSourceInfo returns nil for files. It is present to satisfy the Descriptor
// interface.
func (fd *FileDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return nil
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsFileDescriptorProto, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (fd *FileDescriptor) AsProto() proto.Message {
	return fd.proto
}

// AsFileDescriptorProto returns the underlying descriptor proto.
func (fd *FileDescriptor) AsFileDescriptorProto() *dpb.FileDescriptorProto {
	return fd.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (fd *FileDescriptor) String() string {
	return fd.proto.String()
}

// IsProto3 returns true if the file declares a syntax of "proto3".
func (fd *FileDescriptor) IsProto3() bool {
	return fd.isProto3
}

// GetDependencies returns all of this file's dependencies. These correspond to
// import statements in the file.
func (fd *FileDescriptor) GetDependencies() []*FileDescriptor {
	return fd.deps
}

// GetPublicDependencies returns all of this file's public dependencies. These
// correspond to public import statements in the file.
func (fd *FileDescriptor) GetPublicDependencies() []*FileDescriptor {
	return fd.publicDeps
}

// GetWeakDependencies returns all of this file's weak dependencies. These
// correspond to weak import statements in the file.
func (fd *FileDescriptor) GetWeakDependencies() []*FileDescriptor {
	return fd.weakDeps
}

// GetMessageTypes returns all top-level messages declared in this file.
func (fd *FileDescriptor) GetMessageTypes() []*MessageDescriptor {
	return fd.messages
}

// GetEnumTypes returns all top-level enums declared in this file.
func (fd *FileDescriptor) GetEnumTypes() []*EnumDescriptor {
	return fd.enums
}

// GetExtensions returns all top-level extensions declared in this file.
func (fd *FileDescriptor) GetExtensions() []*FieldDescriptor {
	return fd.extensions
}

// GetServices returns all services declared in this file.
func (fd *FileDescriptor) GetServices() []*ServiceDescriptor {
	return fd.services
}

// FindSymbol returns the descriptor contained within this file for the
// element with the given fully-qualified symbol name. If no such element
// exists then this method returns nil.
func (fd *FileDescriptor) FindSymbol(symbol string) Descriptor {
	if symbol[0] == '.' {
		symbol = symbol[1:]
	}
	if ret := fd.symbols[symbol]; ret != nil {
		return ret
	}

	// allow accessing symbols through public imports, too
	for _, dep := range fd.GetPublicDependencies() {
		if ret := dep.FindSymbol(symbol); ret != nil {
			return ret
		}
	}

	// not found
	return nil
}

// FindMessage finds the message with the given fully-qualified name. If no
// such element exists in this file then nil is returned.
func (fd *FileDescriptor) FindMessage(msgName string) *MessageDescriptor {
	if md, ok := fd.symbols[msgName].(*MessageDescriptor); ok {
		return md
	} else {
		return nil
	}
}

// FindEnum finds the enum with the given fully-qualified name. If no such
// element exists in this file then nil is returned.
func (fd *FileDescriptor) FindEnum(enumName string) *EnumDescriptor {
	if ed, ok := fd.symbols[enumName].(*EnumDescriptor); ok {
		return ed
	} else {
		return nil
	}
}

// FindService finds the service with the given fully-qualified name. If no
// such element exists in this file then nil is returned.
func (fd *FileDescriptor) FindService(serviceName string) *ServiceDescriptor {
	if sd, ok := fd.symbols[serviceName].(*ServiceDescriptor); ok {
		return sd
	} else {
		return nil
	}
}

// FindExtension finds the extension field for the given extended type name and
// tag number. If no such element exists in this file then nil is returned.
func (fd *FileDescriptor) FindExtension(extendeeName string, tagNumber int32) *FieldDescriptor {
	if exd, ok := fd.fieldIndex[extendeeName][tagNumber]; ok && exd.IsExtension() {
		return exd
	} else {
		return nil
	}
}

// FindExtensionByName finds the extension field with the given fully-qualified
// name. If no such element exists in this file then nil is returned.
func (fd *FileDescriptor) FindExtensionByName(extName string) *FieldDescriptor {
	if exd, ok := fd.symbols[extName].(*FieldDescriptor); ok && exd.IsExtension() {
		return exd
	} else {
		return nil
	}
}

// MessageDescriptor describes a protocol buffer message.
type MessageDescriptor struct {
	proto          *dpb.DescriptorProto
	parent         Descriptor
	file           *FileDescriptor
	fields         []*FieldDescriptor
	nested         []*MessageDescriptor
	enums          []*EnumDescriptor
	extensions     []*FieldDescriptor
	oneOfs         []*OneOfDescriptor
	extRanges      extRanges
	fqn            string
	sourceInfoPath []int32
	jsonNames      jsonNameMap
	isProto3       bool
	isMapEntry     bool
}

func createMessageDescriptor(fd *FileDescriptor, parent Descriptor, enclosing string, md *dpb.DescriptorProto, symbols map[string]Descriptor) (*MessageDescriptor, string) {
	msgName := merge(enclosing, md.GetName())
	ret := &MessageDescriptor{proto: md, parent: parent, file: fd, fqn: msgName}
	for _, f := range md.GetField() {
		fld, n := createFieldDescriptor(fd, ret, msgName, f)
		symbols[n] = fld
		ret.fields = append(ret.fields, fld)
	}
	for _, nm := range md.NestedType {
		nmd, n := createMessageDescriptor(fd, ret, msgName, nm, symbols)
		symbols[n] = nmd
		ret.nested = append(ret.nested, nmd)
	}
	for _, e := range md.EnumType {
		ed, n := createEnumDescriptor(fd, ret, msgName, e, symbols)
		symbols[n] = ed
		ret.enums = append(ret.enums, ed)
	}
	for _, ex := range md.GetExtension() {
		exd, n := createFieldDescriptor(fd, ret, msgName, ex)
		symbols[n] = exd
		ret.extensions = append(ret.extensions, exd)
	}
	for i, o := range md.GetOneofDecl() {
		od, n := createOneOfDescriptor(fd, ret, i, msgName, o)
		symbols[n] = od
		ret.oneOfs = append(ret.oneOfs, od)
	}
	for _, r := range md.GetExtensionRange() {
		// proto.ExtensionRange is inclusive (and that's how extension ranges are defined in code).
		// but protoc converts range to exclusive end in descriptor, so we must convert back
		end := r.GetEnd() - 1
		ret.extRanges = append(ret.extRanges, proto.ExtensionRange{
			Start: r.GetStart(),
			End:   end})
	}
	sort.Sort(ret.extRanges)
	ret.isProto3 = fd.isProto3
	ret.isMapEntry = md.GetOptions().GetMapEntry() &&
		len(ret.fields) == 2 &&
		ret.fields[0].GetNumber() == 1 &&
		ret.fields[1].GetNumber() == 2

	return ret, msgName
}

func (md *MessageDescriptor) resolve(path []int32, scopes []scope) error {
	md.sourceInfoPath = append([]int32(nil), path...) // defensive copy
	path = append(path, internal.Message_nestedMessagesTag)
	scopes = append(scopes, messageScope(md))
	for i, nmd := range md.nested {
		if err := nmd.resolve(append(path, int32(i)), scopes); err != nil {
			return err
		}
	}
	path[len(path)-1] = internal.Message_enumsTag
	for i, ed := range md.enums {
		ed.resolve(append(path, int32(i)))
	}
	path[len(path)-1] = internal.Message_fieldsTag
	for i, fld := range md.fields {
		if err := fld.resolve(append(path, int32(i)), scopes); err != nil {
			return err
		}
	}
	path[len(path)-1] = internal.Message_extensionsTag
	for i, exd := range md.extensions {
		if err := exd.resolve(append(path, int32(i)), scopes); err != nil {
			return err
		}
	}
	path[len(path)-1] = internal.Message_oneOfsTag
	for i, od := range md.oneOfs {
		od.resolve(append(path, int32(i)))
	}
	return nil
}

// GetName returns the simple (unqualified) name of the message.
func (md *MessageDescriptor) GetName() string {
	return md.proto.GetName()
}

// GetFullyQualifiedName returns the fully qualified name of the message. This
// includes the package name (if there is one) as well as the names of any
// enclosing messages.
func (md *MessageDescriptor) GetFullyQualifiedName() string {
	return md.fqn
}

// GetParent returns the message's enclosing descriptor. For top-level messages,
// this will be a file descriptor. Otherwise it will be the descriptor for the
// enclosing message.
func (md *MessageDescriptor) GetParent() Descriptor {
	return md.parent
}

// GetFile returns the descriptor for the file in which this message is defined.
func (md *MessageDescriptor) GetFile() *FileDescriptor {
	return md.file
}

// GetOptions returns the message's options. Most usages will be more interested
// in GetMessageOptions, which has a concrete return type. This generic version
// is present to satisfy the Descriptor interface.
func (md *MessageDescriptor) GetOptions() proto.Message {
	return md.proto.GetOptions()
}

// GetMessageOptions returns the message's options.
func (md *MessageDescriptor) GetMessageOptions() *dpb.MessageOptions {
	return md.proto.GetOptions()
}

// GetSourceInfo returns source info for the message, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// message was defined and also contains comments associated with the message
// definition.
func (md *MessageDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return md.file.sourceInfo.Get(md.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsDescriptorProto, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (md *MessageDescriptor) AsProto() proto.Message {
	return md.proto
}

// AsDescriptorProto returns the underlying descriptor proto.
func (md *MessageDescriptor) AsDescriptorProto() *dpb.DescriptorProto {
	return md.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (md *MessageDescriptor) String() string {
	return md.proto.String()
}

// IsMapEntry returns true if this is a synthetic message type that represents an entry
// in a map field.
func (md *MessageDescriptor) IsMapEntry() bool {
	return md.isMapEntry
}

// GetFields returns all of the fields for this message.
func (md *MessageDescriptor) GetFields() []*FieldDescriptor {
	return md.fields
}

// GetNestedMessageTypes returns all of the message types declared inside this message.
func (md *MessageDescriptor) GetNestedMessageTypes() []*MessageDescriptor {
	return md.nested
}

// GetNestedEnumTypes returns all of the enums declared inside this message.
func (md *MessageDescriptor) GetNestedEnumTypes() []*EnumDescriptor {
	return md.enums
}

// GetNestedExtensions returns all of the extensions declared inside this message.
func (md *MessageDescriptor) GetNestedExtensions() []*FieldDescriptor {
	return md.extensions
}

// GetOneOfs returns all of the one-of field sets declared inside this message.
func (md *MessageDescriptor) GetOneOfs() []*OneOfDescriptor {
	return md.oneOfs
}

// IsProto3 returns true if the file in which this message is defined declares a syntax of "proto3".
func (md *MessageDescriptor) IsProto3() bool {
	return md.isProto3
}

// GetExtensionRanges returns the ranges of extension field numbers for this message.
func (md *MessageDescriptor) GetExtensionRanges() []proto.ExtensionRange {
	return md.extRanges
}

// IsExtendable returns true if this message has any extension ranges.
func (md *MessageDescriptor) IsExtendable() bool {
	return len(md.extRanges) > 0
}

// IsExtension returns true if the given tag number is within any of this message's
// extension ranges.
func (md *MessageDescriptor) IsExtension(tagNumber int32) bool {
	return md.extRanges.IsExtension(tagNumber)
}

type extRanges []proto.ExtensionRange

func (er extRanges) String() string {
	var buf bytes.Buffer
	first := true
	for _, r := range er {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%d..%d", r.Start, r.End)
	}
	return buf.String()
}

func (er extRanges) IsExtension(tagNumber int32) bool {
	i := sort.Search(len(er), func(i int) bool { return er[i].End >= tagNumber })
	return i < len(er) && tagNumber >= er[i].Start
}

func (er extRanges) Len() int {
	return len(er)
}

func (er extRanges) Less(i, j int) bool {
	return er[i].Start < er[j].Start
}

func (er extRanges) Swap(i, j int) {
	er[i], er[j] = er[j], er[i]
}

// FindFieldByName finds the field with the given name. If no such field exists
// then nil is returned. Only regular fields are returned, not extensions.
func (md *MessageDescriptor) FindFieldByName(fieldName string) *FieldDescriptor {
	fqn := fmt.Sprintf("%s.%s", md.fqn, fieldName)
	if fd, ok := md.file.symbols[fqn].(*FieldDescriptor); ok && !fd.IsExtension() {
		return fd
	} else {
		return nil
	}
}

// FindFieldByNumber finds the field with the given tag number. If no such field
// exists then nil is returned. Only regular fields are returned, not extensions.
func (md *MessageDescriptor) FindFieldByNumber(tagNumber int32) *FieldDescriptor {
	if fd, ok := md.file.fieldIndex[md.fqn][tagNumber]; ok && !fd.IsExtension() {
		return fd
	} else {
		return nil
	}
}

// FieldDescriptor describes a field of a protocol buffer message.
type FieldDescriptor struct {
	proto          *dpb.FieldDescriptorProto
	parent         Descriptor
	owner          *MessageDescriptor
	file           *FileDescriptor
	oneOf          *OneOfDescriptor
	msgType        *MessageDescriptor
	enumType       *EnumDescriptor
	fqn            string
	sourceInfoPath []int32
	def            memoizedDefault
	isMap          bool
}

func createFieldDescriptor(fd *FileDescriptor, parent Descriptor, enclosing string, fld *dpb.FieldDescriptorProto) (*FieldDescriptor, string) {
	fldName := merge(enclosing, fld.GetName())
	ret := &FieldDescriptor{proto: fld, parent: parent, file: fd, fqn: fldName}
	if fld.GetExtendee() == "" {
		ret.owner = parent.(*MessageDescriptor)
	}
	// owner for extensions, field type (be it message or enum), and one-ofs get resolved later
	return ret, fldName
}

func (fd *FieldDescriptor) resolve(path []int32, scopes []scope) error {
	if fd.proto.OneofIndex != nil && fd.oneOf == nil {
		return fmt.Errorf("could not link field %s to one-of index %d", fd.fqn, *fd.proto.OneofIndex)
	}
	fd.sourceInfoPath = append([]int32(nil), path...) // defensive copy
	if fd.proto.GetType() == dpb.FieldDescriptorProto_TYPE_ENUM {
		if desc, err := resolve(fd.file, fd.proto.GetTypeName(), scopes); err != nil {
			return err
		} else {
			fd.enumType = desc.(*EnumDescriptor)
		}
	}
	if fd.proto.GetType() == dpb.FieldDescriptorProto_TYPE_MESSAGE || fd.proto.GetType() == dpb.FieldDescriptorProto_TYPE_GROUP {
		if desc, err := resolve(fd.file, fd.proto.GetTypeName(), scopes); err != nil {
			return err
		} else {
			fd.msgType = desc.(*MessageDescriptor)
		}
	}
	if fd.proto.GetExtendee() != "" {
		if desc, err := resolve(fd.file, fd.proto.GetExtendee(), scopes); err != nil {
			return err
		} else {
			fd.owner = desc.(*MessageDescriptor)
		}
	}
	fd.file.registerField(fd)
	fd.isMap = fd.proto.GetLabel() == dpb.FieldDescriptorProto_LABEL_REPEATED &&
		fd.proto.GetType() == dpb.FieldDescriptorProto_TYPE_MESSAGE &&
		fd.GetMessageType().IsMapEntry()
	return nil
}

func (fd *FieldDescriptor) determineDefault() interface{} {
	if fd.IsMap() {
		return map[interface{}]interface{}(nil)
	} else if fd.IsRepeated() {
		return []interface{}(nil)
	} else if fd.msgType != nil {
		return nil
	}

	proto3 := fd.file.isProto3
	if !proto3 {
		def := fd.AsFieldDescriptorProto().GetDefaultValue()
		if def != "" {
			ret := parseDefaultValue(fd, def)
			if ret != nil {
				return ret
			}
			// if we can't parse default value, fall-through to return normal default...
		}
	}

	switch fd.GetType() {
	case dpb.FieldDescriptorProto_TYPE_FIXED32,
		dpb.FieldDescriptorProto_TYPE_UINT32:
		return uint32(0)
	case dpb.FieldDescriptorProto_TYPE_SFIXED32,
		dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_SINT32:
		return int32(0)
	case dpb.FieldDescriptorProto_TYPE_FIXED64,
		dpb.FieldDescriptorProto_TYPE_UINT64:
		return uint64(0)
	case dpb.FieldDescriptorProto_TYPE_SFIXED64,
		dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_SINT64:
		return int64(0)
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		return float32(0.0)
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		return float64(0.0)
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		return false
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		return []byte(nil)
	case dpb.FieldDescriptorProto_TYPE_STRING:
		return ""
	case dpb.FieldDescriptorProto_TYPE_ENUM:
		if proto3 {
			return int32(0)
		}
		enumVals := fd.GetEnumType().GetValues()
		if len(enumVals) > 0 {
			return enumVals[0].GetNumber()
		} else {
			return int32(0) // WTF?
		}
	default:
		panic(fmt.Sprintf("Unknown field type: %v", fd.GetType()))
	}
}

func parseDefaultValue(fd *FieldDescriptor, val string) interface{} {
	switch fd.GetType() {
	case dpb.FieldDescriptorProto_TYPE_ENUM:
		vd := fd.GetEnumType().FindValueByName(val)
		if vd != nil {
			return vd.GetNumber()
		}
		return nil
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		if val == "true" {
			return true
		} else if val == "false" {
			return false
		}
		return nil
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		return []byte(unescape(val))
	case dpb.FieldDescriptorProto_TYPE_STRING:
		return val
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		if f, err := strconv.ParseFloat(val, 32); err == nil {
			return float32(f)
		} else {
			return float32(0)
		}
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		} else {
			return float64(0)
		}
	case dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_SINT32,
		dpb.FieldDescriptorProto_TYPE_SFIXED32:
		if i, err := strconv.ParseInt(val, 10, 32); err == nil {
			return int32(i)
		} else {
			return int32(0)
		}
	case dpb.FieldDescriptorProto_TYPE_UINT32,
		dpb.FieldDescriptorProto_TYPE_FIXED32:
		if i, err := strconv.ParseUint(val, 10, 32); err == nil {
			return uint32(i)
		} else {
			return uint32(0)
		}
	case dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_SINT64,
		dpb.FieldDescriptorProto_TYPE_SFIXED64:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		} else {
			return int64(0)
		}
	case dpb.FieldDescriptorProto_TYPE_UINT64,
		dpb.FieldDescriptorProto_TYPE_FIXED64:
		if i, err := strconv.ParseUint(val, 10, 64); err == nil {
			return i
		} else {
			return uint64(0)
		}
	default:
		return nil
	}
}

func unescape(s string) string {
	// protoc encodes default values for 'bytes' fields using C escaping,
	// so this function reverses that escaping
	out := make([]byte, 0, len(s))
	var buf [4]byte
	for len(s) > 0 {
		if s[0] != '\\' || len(s) < 2 {
			// not escape sequence, or too short to be well-formed escape
			out = append(out, s[0])
			s = s[1:]
		} else if s[1] == 'x' || s[1] == 'X' {
			n := matchPrefix(s[2:], 2, isHex)
			if n == 0 {
				// bad escape
				out = append(out, s[:2]...)
				s = s[2:]
			} else {
				c, err := strconv.ParseUint(s[2:2+n], 16, 8)
				if err != nil {
					// shouldn't really happen...
					out = append(out, s[:2+n]...)
				} else {
					out = append(out, byte(c))
				}
				s = s[2+n:]
			}
		} else if s[1] >= '0' && s[1] <= '7' {
			n := 1 + matchPrefix(s[2:], 2, isOctal)
			c, err := strconv.ParseUint(s[1:1+n], 8, 8)
			if err != nil || c > 0xff {
				out = append(out, s[:1+n]...)
			} else {
				out = append(out, byte(c))
			}
			s = s[1+n:]
		} else if s[1] == 'u' {
			if len(s) < 6 {
				// bad escape
				out = append(out, s...)
				s = s[len(s):]
			} else {
				c, err := strconv.ParseUint(s[2:6], 16, 16)
				if err != nil {
					// bad escape
					out = append(out, s[:6]...)
				} else {
					w := utf8.EncodeRune(buf[:], rune(c))
					out = append(out, buf[:w]...)
				}
				s = s[6:]
			}
		} else if s[1] == 'U' {
			if len(s) < 10 {
				// bad escape
				out = append(out, s...)
				s = s[len(s):]
			} else {
				c, err := strconv.ParseUint(s[2:10], 16, 32)
				if err != nil || c > 0x10ffff {
					// bad escape
					out = append(out, s[:10]...)
				} else {
					w := utf8.EncodeRune(buf[:], rune(c))
					out = append(out, buf[:w]...)
				}
				s = s[10:]
			}
		} else {
			switch s[1] {
			case 'a':
				out = append(out, '\a')
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case 'v':
				out = append(out, '\v')
			case '\\':
				out = append(out, '\\')
			case '\'':
				out = append(out, '\'')
			case '"':
				out = append(out, '"')
			case '?':
				out = append(out, '?')
			default:
				// invalid escape, just copy it as-is
				out = append(out, s[:2]...)
			}
			s = s[2:]
		}
	}
	return string(out)
}

func isOctal(b byte) bool { return b >= '0' && b <= '7' }
func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}
func matchPrefix(s string, limit int, fn func(byte) bool) int {
	l := len(s)
	if l > limit {
		l = limit
	}
	i := 0
	for ; i < l; i++ {
		if !fn(s[i]) {
			return i
		}
	}
	return i
}

// GetName returns the name of the field.
func (fd *FieldDescriptor) GetName() string {
	return fd.proto.GetName()
}

// GetNumber returns the tag number of this field.
func (fd *FieldDescriptor) GetNumber() int32 {
	return fd.proto.GetNumber()
}

// GetFullyQualifiedName returns the fully qualified name of the field. Unlike
// GetName, this includes fully qualified name of the enclosing message for
// regular fields.
//
// For extension fields, this includes the package (if there is one) as well as
// any enclosing messages. The package and/or enclosing messages are for where
// the extension is defined, not the message it extends.
//
// If this field is part of a one-of, the fully qualified name does *not*
// include the name of the one-of, only of the enclosing message.
func (fd *FieldDescriptor) GetFullyQualifiedName() string {
	return fd.fqn
}

// GetParent returns the fields's enclosing descriptor. For normal
// (non-extension) fields, this is the enclosing message. For extensions, this
// is the descriptor in which the extension is defined, not the message that is
// extended. The parent for an extension may be a file descriptor or a message,
// depending on where the extension is defined.
func (fd *FieldDescriptor) GetParent() Descriptor {
	return fd.parent
}

// GetFile returns the descriptor for the file in which this field is defined.
func (fd *FieldDescriptor) GetFile() *FileDescriptor {
	return fd.file
}

// GetOptions returns the field's options. Most usages will be more interested
// in GetFieldOptions, which has a concrete return type. This generic version
// is present to satisfy the Descriptor interface.
func (fd *FieldDescriptor) GetOptions() proto.Message {
	return fd.proto.GetOptions()
}

// GetFieldOptions returns the field's options.
func (fd *FieldDescriptor) GetFieldOptions() *dpb.FieldOptions {
	return fd.proto.GetOptions()
}

// GetSourceInfo returns source info for the field, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// field was defined and also contains comments associated with the field
// definition.
func (fd *FieldDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return fd.file.sourceInfo.Get(fd.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsFieldDescriptorProto, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (fd *FieldDescriptor) AsProto() proto.Message {
	return fd.proto
}

// AsFieldDescriptorProto returns the underlying descriptor proto.
func (fd *FieldDescriptor) AsFieldDescriptorProto() *dpb.FieldDescriptorProto {
	return fd.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (fd *FieldDescriptor) String() string {
	return fd.proto.String()
}

// GetJSONName returns the name of the field as referenced in the message's JSON
// format.
func (fd *FieldDescriptor) GetJSONName() string {
	if jsonName := fd.proto.JsonName; jsonName != nil {
		// if json name is present, use its value
		return *jsonName
	}
	// otherwise, compute the proper JSON name from the field name
	return jsonCamelCase(fd.proto.GetName())
}

func jsonCamelCase(s string) string {
	// This mirrors the implementation in protoc/C++ runtime and in the Java runtime:
	//   https://github.com/protocolbuffers/protobuf/blob/a104dffcb6b1958a424f5fa6f9e6bdc0ab9b6f9e/src/google/protobuf/descriptor.cc#L276
	//   https://github.com/protocolbuffers/protobuf/blob/a1c886834425abb64a966231dd2c9dd84fb289b3/java/core/src/main/java/com/google/protobuf/Descriptors.java#L1286
	var buf bytes.Buffer
	prevWasUnderscore := false
	for _, r := range s {
		if r == '_' {
			prevWasUnderscore = true
			continue
		}
		if prevWasUnderscore {
			r = unicode.ToUpper(r)
			prevWasUnderscore = false
		}
		buf.WriteRune(r)
	}
	return buf.String()
}

// GetFullyQualifiedJSONName returns the JSON format name (same as GetJSONName),
// but includes the fully qualified name of the enclosing message.
//
// If the field is an extension, it will return the package name (if there is
// one) as well as the names of any enclosing messages. The package and/or
// enclosing messages are for where the extension is defined, not the message it
// extends.
func (fd *FieldDescriptor) GetFullyQualifiedJSONName() string {
	parent := fd.GetParent()
	switch parent := parent.(type) {
	case *FileDescriptor:
		pkg := parent.GetPackage()
		if pkg == "" {
			return fd.GetJSONName()
		}
		return fmt.Sprintf("%s.%s", pkg, fd.GetJSONName())
	default:
		return fmt.Sprintf("%s.%s", parent.GetFullyQualifiedName(), fd.GetJSONName())
	}
}

// GetOwner returns the message type that this field belongs to. If this is a normal
// field then this is the same as GetParent. But for extensions, this will be the
// extendee message whereas GetParent refers to where the extension was declared.
func (fd *FieldDescriptor) GetOwner() *MessageDescriptor {
	return fd.owner
}

// IsExtension returns true if this is an extension field.
func (fd *FieldDescriptor) IsExtension() bool {
	return fd.proto.GetExtendee() != ""
}

// GetOneOf returns the one-of field set to which this field belongs. If this field
// is not part of a one-of then this method returns nil.
func (fd *FieldDescriptor) GetOneOf() *OneOfDescriptor {
	return fd.oneOf
}

// GetType returns the type of this field. If the type indicates an enum, the
// enum type can be queried via GetEnumType. If the type indicates a message, the
// message type can be queried via GetMessageType.
func (fd *FieldDescriptor) GetType() dpb.FieldDescriptorProto_Type {
	return fd.proto.GetType()
}

// GetLabel returns the label for this field. The label can be required (proto2-only),
// optional (default for proto3), or required.
func (fd *FieldDescriptor) GetLabel() dpb.FieldDescriptorProto_Label {
	return fd.proto.GetLabel()
}

// IsRequired returns true if this field has the "required" label.
func (fd *FieldDescriptor) IsRequired() bool {
	return fd.proto.GetLabel() == dpb.FieldDescriptorProto_LABEL_REQUIRED
}

// IsRepeated returns true if this field has the "repeated" label.
func (fd *FieldDescriptor) IsRepeated() bool {
	return fd.proto.GetLabel() == dpb.FieldDescriptorProto_LABEL_REPEATED
}

// IsProto3Optional returns true if this field has an explicit "optional" label
// and is in a "proto3" syntax file. Such fields, if they are normal fields (not
// extensions), will be nested in synthetic oneofs that contain only the single
// field.
func (fd *FieldDescriptor) IsProto3Optional() bool {
	return internal.GetProto3Optional(fd.proto)
}

// HasPresence returns true if this field can distinguish when a value is
// present or not. Scalar fields in "proto3" syntax files, for example, return
// false since absent values are indistinguishable from zero values.
func (fd *FieldDescriptor) HasPresence() bool {
	if !fd.file.isProto3 {
		return true
	}
	return fd.msgType != nil || fd.oneOf != nil
}

// IsMap returns true if this is a map field. If so, it will have the "repeated"
// label its type will be a message that represents a map entry. The map entry
// message will have exactly two fields: tag #1 is the key and tag #2 is the value.
func (fd *FieldDescriptor) IsMap() bool {
	return fd.isMap
}

// GetMapKeyType returns the type of the key field if this is a map field. If it is
// not a map field, nil is returned.
func (fd *FieldDescriptor) GetMapKeyType() *FieldDescriptor {
	if fd.isMap {
		return fd.msgType.FindFieldByNumber(int32(1))
	}
	return nil
}

// GetMapValueType returns the type of the value field if this is a map field. If it
// is not a map field, nil is returned.
func (fd *FieldDescriptor) GetMapValueType() *FieldDescriptor {
	if fd.isMap {
		return fd.msgType.FindFieldByNumber(int32(2))
	}
	return nil
}

// GetMessageType returns the type of this field if it is a message type. If
// this field is not a message type, it returns nil.
func (fd *FieldDescriptor) GetMessageType() *MessageDescriptor {
	return fd.msgType
}

// GetEnumType returns the type of this field if it is an enum type. If this
// field is not an enum type, it returns nil.
func (fd *FieldDescriptor) GetEnumType() *EnumDescriptor {
	return fd.enumType
}

// GetDefaultValue returns the default value for this field.
//
// If this field represents a message type, this method always returns nil (even though
// for proto2 files, the default value should be a default instance of the message type).
// If the field represents an enum type, this method returns an int32 corresponding to the
// enum value. If this field is a map, it returns a nil map[interface{}]interface{}. If
// this field is repeated (and not a map), it returns a nil []interface{}.
//
// Otherwise, it returns the declared default value for the field or a zero value, if no
// default is declared or if the file is proto3. The type of said return value corresponds
// to the type of the field:
//  +-------------------------+-----------+
//  |       Declared Type     |  Go Type  |
//  +-------------------------+-----------+
//  | int32, sint32, sfixed32 | int32     |
//  | int64, sint64, sfixed64 | int64     |
//  | uint32, fixed32         | uint32    |
//  | uint64, fixed64         | uint64    |
//  | float                   | float32   |
//  | double                  | double32  |
//  | bool                    | bool      |
//  | string                  | string    |
//  | bytes                   | []byte    |
//  +-------------------------+-----------+
func (fd *FieldDescriptor) GetDefaultValue() interface{} {
	return fd.getDefaultValue()
}

// EnumDescriptor describes an enum declared in a proto file.
type EnumDescriptor struct {
	proto          *dpb.EnumDescriptorProto
	parent         Descriptor
	file           *FileDescriptor
	values         []*EnumValueDescriptor
	valuesByNum    sortedValues
	fqn            string
	sourceInfoPath []int32
}

func createEnumDescriptor(fd *FileDescriptor, parent Descriptor, enclosing string, ed *dpb.EnumDescriptorProto, symbols map[string]Descriptor) (*EnumDescriptor, string) {
	enumName := merge(enclosing, ed.GetName())
	ret := &EnumDescriptor{proto: ed, parent: parent, file: fd, fqn: enumName}
	for _, ev := range ed.GetValue() {
		evd, n := createEnumValueDescriptor(fd, ret, enumName, ev)
		symbols[n] = evd
		ret.values = append(ret.values, evd)
	}
	if len(ret.values) > 0 {
		ret.valuesByNum = make(sortedValues, len(ret.values))
		copy(ret.valuesByNum, ret.values)
		sort.Stable(ret.valuesByNum)
	}
	return ret, enumName
}

type sortedValues []*EnumValueDescriptor

func (sv sortedValues) Len() int {
	return len(sv)
}

func (sv sortedValues) Less(i, j int) bool {
	return sv[i].GetNumber() < sv[j].GetNumber()
}

func (sv sortedValues) Swap(i, j int) {
	sv[i], sv[j] = sv[j], sv[i]
}

func (ed *EnumDescriptor) resolve(path []int32) {
	ed.sourceInfoPath = append([]int32(nil), path...) // defensive copy
	path = append(path, internal.Enum_valuesTag)
	for i, evd := range ed.values {
		evd.resolve(append(path, int32(i)))
	}
}

// GetName returns the simple (unqualified) name of the enum type.
func (ed *EnumDescriptor) GetName() string {
	return ed.proto.GetName()
}

// GetFullyQualifiedName returns the fully qualified name of the enum type.
// This includes the package name (if there is one) as well as the names of any
// enclosing messages.
func (ed *EnumDescriptor) GetFullyQualifiedName() string {
	return ed.fqn
}

// GetParent returns the enum type's enclosing descriptor. For top-level enums,
// this will be a file descriptor. Otherwise it will be the descriptor for the
// enclosing message.
func (ed *EnumDescriptor) GetParent() Descriptor {
	return ed.parent
}

// GetFile returns the descriptor for the file in which this enum is defined.
func (ed *EnumDescriptor) GetFile() *FileDescriptor {
	return ed.file
}

// GetOptions returns the enum type's options. Most usages will be more
// interested in GetEnumOptions, which has a concrete return type. This generic
// version is present to satisfy the Descriptor interface.
func (ed *EnumDescriptor) GetOptions() proto.Message {
	return ed.proto.GetOptions()
}

// GetEnumOptions returns the enum type's options.
func (ed *EnumDescriptor) GetEnumOptions() *dpb.EnumOptions {
	return ed.proto.GetOptions()
}

// GetSourceInfo returns source info for the enum type, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// enum type was defined and also contains comments associated with the enum
// definition.
func (ed *EnumDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return ed.file.sourceInfo.Get(ed.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsEnumDescriptorProto, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (ed *EnumDescriptor) AsProto() proto.Message {
	return ed.proto
}

// AsEnumDescriptorProto returns the underlying descriptor proto.
func (ed *EnumDescriptor) AsEnumDescriptorProto() *dpb.EnumDescriptorProto {
	return ed.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (ed *EnumDescriptor) String() string {
	return ed.proto.String()
}

// GetValues returns all of the allowed values defined for this enum.
func (ed *EnumDescriptor) GetValues() []*EnumValueDescriptor {
	return ed.values
}

// FindValueByName finds the enum value with the given name. If no such value exists
// then nil is returned.
func (ed *EnumDescriptor) FindValueByName(name string) *EnumValueDescriptor {
	fqn := fmt.Sprintf("%s.%s", ed.fqn, name)
	if vd, ok := ed.file.symbols[fqn].(*EnumValueDescriptor); ok {
		return vd
	} else {
		return nil
	}
}

// FindValueByNumber finds the value with the given numeric value. If no such value
// exists then nil is returned. If aliases are allowed and multiple values have the
// given number, the first declared value is returned.
func (ed *EnumDescriptor) FindValueByNumber(num int32) *EnumValueDescriptor {
	index := sort.Search(len(ed.valuesByNum), func(i int) bool { return ed.valuesByNum[i].GetNumber() >= num })
	if index < len(ed.valuesByNum) {
		vd := ed.valuesByNum[index]
		if vd.GetNumber() == num {
			return vd
		}
	}
	return nil
}

// EnumValueDescriptor describes an allowed value of an enum declared in a proto file.
type EnumValueDescriptor struct {
	proto          *dpb.EnumValueDescriptorProto
	parent         *EnumDescriptor
	file           *FileDescriptor
	fqn            string
	sourceInfoPath []int32
}

func createEnumValueDescriptor(fd *FileDescriptor, parent *EnumDescriptor, enclosing string, evd *dpb.EnumValueDescriptorProto) (*EnumValueDescriptor, string) {
	valName := merge(enclosing, evd.GetName())
	return &EnumValueDescriptor{proto: evd, parent: parent, file: fd, fqn: valName}, valName
}

func (vd *EnumValueDescriptor) resolve(path []int32) {
	vd.sourceInfoPath = append([]int32(nil), path...) // defensive copy
}

// GetName returns the name of the enum value.
func (vd *EnumValueDescriptor) GetName() string {
	return vd.proto.GetName()
}

// GetNumber returns the numeric value associated with this enum value.
func (vd *EnumValueDescriptor) GetNumber() int32 {
	return vd.proto.GetNumber()
}

// GetFullyQualifiedName returns the fully qualified name of the enum value.
// Unlike GetName, this includes fully qualified name of the enclosing enum.
func (vd *EnumValueDescriptor) GetFullyQualifiedName() string {
	return vd.fqn
}

// GetParent returns the descriptor for the enum in which this enum value is
// defined. Most usages will prefer to use GetEnum, which has a concrete return
// type. This more generic method is present to satisfy the Descriptor interface.
func (vd *EnumValueDescriptor) GetParent() Descriptor {
	return vd.parent
}

// GetEnum returns the enum in which this enum value is defined.
func (vd *EnumValueDescriptor) GetEnum() *EnumDescriptor {
	return vd.parent
}

// GetFile returns the descriptor for the file in which this enum value is
// defined.
func (vd *EnumValueDescriptor) GetFile() *FileDescriptor {
	return vd.file
}

// GetOptions returns the enum value's options. Most usages will be more
// interested in GetEnumValueOptions, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (vd *EnumValueDescriptor) GetOptions() proto.Message {
	return vd.proto.GetOptions()
}

// GetEnumValueOptions returns the enum value's options.
func (vd *EnumValueDescriptor) GetEnumValueOptions() *dpb.EnumValueOptions {
	return vd.proto.GetOptions()
}

// GetSourceInfo returns source info for the enum value, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// enum value was defined and also contains comments associated with the enum
// value definition.
func (vd *EnumValueDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return vd.file.sourceInfo.Get(vd.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsEnumValueDescriptorProto, which has a concrete return type.
// This generic version is present to satisfy the Descriptor interface.
func (vd *EnumValueDescriptor) AsProto() proto.Message {
	return vd.proto
}

// AsEnumValueDescriptorProto returns the underlying descriptor proto.
func (vd *EnumValueDescriptor) AsEnumValueDescriptorProto() *dpb.EnumValueDescriptorProto {
	return vd.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (vd *EnumValueDescriptor) String() string {
	return vd.proto.String()
}

// ServiceDescriptor describes an RPC service declared in a proto file.
type ServiceDescriptor struct {
	proto          *dpb.ServiceDescriptorProto
	file           *FileDescriptor
	methods        []*MethodDescriptor
	fqn            string
	sourceInfoPath []int32
}

func createServiceDescriptor(fd *FileDescriptor, enclosing string, sd *dpb.ServiceDescriptorProto, symbols map[string]Descriptor) (*ServiceDescriptor, string) {
	serviceName := merge(enclosing, sd.GetName())
	ret := &ServiceDescriptor{proto: sd, file: fd, fqn: serviceName}
	for _, m := range sd.GetMethod() {
		md, n := createMethodDescriptor(fd, ret, serviceName, m)
		symbols[n] = md
		ret.methods = append(ret.methods, md)
	}
	return ret, serviceName
}

func (sd *ServiceDescriptor) resolve(path []int32, scopes []scope) error {
	sd.sourceInfoPath = append([]int32(nil), path...) // defensive copy
	path = append(path, internal.Service_methodsTag)
	for i, md := range sd.methods {
		if err := md.resolve(append(path, int32(i)), scopes); err != nil {
			return err
		}
	}
	return nil
}

// GetName returns the simple (unqualified) name of the service.
func (sd *ServiceDescriptor) GetName() string {
	return sd.proto.GetName()
}

// GetFullyQualifiedName returns the fully qualified name of the service. This
// includes the package name (if there is one).
func (sd *ServiceDescriptor) GetFullyQualifiedName() string {
	return sd.fqn
}

// GetParent returns the descriptor for the file in which this service is
// defined. Most usages will prefer to use GetFile, which has a concrete return
// type. This more generic method is present to satisfy the Descriptor interface.
func (sd *ServiceDescriptor) GetParent() Descriptor {
	return sd.file
}

// GetFile returns the descriptor for the file in which this service is defined.
func (sd *ServiceDescriptor) GetFile() *FileDescriptor {
	return sd.file
}

// GetOptions returns the service's options. Most usages will be more interested
// in GetServiceOptions, which has a concrete return type. This generic version
// is present to satisfy the Descriptor interface.
func (sd *ServiceDescriptor) GetOptions() proto.Message {
	return sd.proto.GetOptions()
}

// GetServiceOptions returns the service's options.
func (sd *ServiceDescriptor) GetServiceOptions() *dpb.ServiceOptions {
	return sd.proto.GetOptions()
}

// GetSourceInfo returns source info for the service, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// service was defined and also contains comments associated with the service
// definition.
func (sd *ServiceDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return sd.file.sourceInfo.Get(sd.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsServiceDescriptorProto, which has a concrete return type.
// This generic version is present to satisfy the Descriptor interface.
func (sd *ServiceDescriptor) AsProto() proto.Message {
	return sd.proto
}

// AsServiceDescriptorProto returns the underlying descriptor proto.
func (sd *ServiceDescriptor) AsServiceDescriptorProto() *dpb.ServiceDescriptorProto {
	return sd.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (sd *ServiceDescriptor) String() string {
	return sd.proto.String()
}

// GetMethods returns all of the RPC methods for this service.
func (sd *ServiceDescriptor) GetMethods() []*MethodDescriptor {
	return sd.methods
}

// FindMethodByName finds the method with the given name. If no such method exists
// then nil is returned.
func (sd *ServiceDescriptor) FindMethodByName(name string) *MethodDescriptor {
	fqn := fmt.Sprintf("%s.%s", sd.fqn, name)
	if md, ok := sd.file.symbols[fqn].(*MethodDescriptor); ok {
		return md
	} else {
		return nil
	}
}

// MethodDescriptor describes an RPC method declared in a proto file.
type MethodDescriptor struct {
	proto          *dpb.MethodDescriptorProto
	parent         *ServiceDescriptor
	file           *FileDescriptor
	inType         *MessageDescriptor
	outType        *MessageDescriptor
	fqn            string
	sourceInfoPath []int32
}

func createMethodDescriptor(fd *FileDescriptor, parent *ServiceDescriptor, enclosing string, md *dpb.MethodDescriptorProto) (*MethodDescriptor, string) {
	// request and response types get resolved later
	methodName := merge(enclosing, md.GetName())
	return &MethodDescriptor{proto: md, parent: parent, file: fd, fqn: methodName}, methodName
}

func (md *MethodDescriptor) resolve(path []int32, scopes []scope) error {
	md.sourceInfoPath = append([]int32(nil), path...) // defensive copy
	if desc, err := resolve(md.file, md.proto.GetInputType(), scopes); err != nil {
		return err
	} else {
		md.inType = desc.(*MessageDescriptor)
	}
	if desc, err := resolve(md.file, md.proto.GetOutputType(), scopes); err != nil {
		return err
	} else {
		md.outType = desc.(*MessageDescriptor)
	}
	return nil
}

// GetName returns the name of the method.
func (md *MethodDescriptor) GetName() string {
	return md.proto.GetName()
}

// GetFullyQualifiedName returns the fully qualified name of the method. Unlike
// GetName, this includes fully qualified name of the enclosing service.
func (md *MethodDescriptor) GetFullyQualifiedName() string {
	return md.fqn
}

// GetParent returns the descriptor for the service in which this method is
// defined. Most usages will prefer to use GetService, which has a concrete
// return type. This more generic method is present to satisfy the Descriptor
// interface.
func (md *MethodDescriptor) GetParent() Descriptor {
	return md.parent
}

// GetService returns the RPC service in which this method is declared.
func (md *MethodDescriptor) GetService() *ServiceDescriptor {
	return md.parent
}

// GetFile returns the descriptor for the file in which this method is defined.
func (md *MethodDescriptor) GetFile() *FileDescriptor {
	return md.file
}

// GetOptions returns the method's options. Most usages will be more interested
// in GetMethodOptions, which has a concrete return type. This generic version
// is present to satisfy the Descriptor interface.
func (md *MethodDescriptor) GetOptions() proto.Message {
	return md.proto.GetOptions()
}

// GetMethodOptions returns the method's options.
func (md *MethodDescriptor) GetMethodOptions() *dpb.MethodOptions {
	return md.proto.GetOptions()
}

// GetSourceInfo returns source info for the method, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// method was defined and also contains comments associated with the method
// definition.
func (md *MethodDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return md.file.sourceInfo.Get(md.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsMethodDescriptorProto, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (md *MethodDescriptor) AsProto() proto.Message {
	return md.proto
}

// AsMethodDescriptorProto returns the underlying descriptor proto.
func (md *MethodDescriptor) AsMethodDescriptorProto() *dpb.MethodDescriptorProto {
	return md.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (md *MethodDescriptor) String() string {
	return md.proto.String()
}

// IsServerStreaming returns true if this is a server-streaming method.
func (md *MethodDescriptor) IsServerStreaming() bool {
	return md.proto.GetServerStreaming()
}

// IsClientStreaming returns true if this is a client-streaming method.
func (md *MethodDescriptor) IsClientStreaming() bool {
	return md.proto.GetClientStreaming()
}

// GetInputType returns the input type, or request type, of the RPC method.
func (md *MethodDescriptor) GetInputType() *MessageDescriptor {
	return md.inType
}

// GetOutputType returns the output type, or response type, of the RPC method.
func (md *MethodDescriptor) GetOutputType() *MessageDescriptor {
	return md.outType
}

// OneOfDescriptor describes a one-of field set declared in a protocol buffer message.
type OneOfDescriptor struct {
	proto          *dpb.OneofDescriptorProto
	parent         *MessageDescriptor
	file           *FileDescriptor
	choices        []*FieldDescriptor
	fqn            string
	sourceInfoPath []int32
}

func createOneOfDescriptor(fd *FileDescriptor, parent *MessageDescriptor, index int, enclosing string, od *dpb.OneofDescriptorProto) (*OneOfDescriptor, string) {
	oneOfName := merge(enclosing, od.GetName())
	ret := &OneOfDescriptor{proto: od, parent: parent, file: fd, fqn: oneOfName}
	for _, f := range parent.fields {
		oi := f.proto.OneofIndex
		if oi != nil && *oi == int32(index) {
			f.oneOf = ret
			ret.choices = append(ret.choices, f)
		}
	}
	return ret, oneOfName
}

func (od *OneOfDescriptor) resolve(path []int32) {
	od.sourceInfoPath = append([]int32(nil), path...) // defensive copy
}

// GetName returns the name of the one-of.
func (od *OneOfDescriptor) GetName() string {
	return od.proto.GetName()
}

// GetFullyQualifiedName returns the fully qualified name of the one-of. Unlike
// GetName, this includes fully qualified name of the enclosing message.
func (od *OneOfDescriptor) GetFullyQualifiedName() string {
	return od.fqn
}

// GetParent returns the descriptor for the message in which this one-of is
// defined. Most usages will prefer to use GetOwner, which has a concrete
// return type. This more generic method is present to satisfy the Descriptor
// interface.
func (od *OneOfDescriptor) GetParent() Descriptor {
	return od.parent
}

// GetOwner returns the message to which this one-of field set belongs.
func (od *OneOfDescriptor) GetOwner() *MessageDescriptor {
	return od.parent
}

// GetFile returns the descriptor for the file in which this one-fof is defined.
func (od *OneOfDescriptor) GetFile() *FileDescriptor {
	return od.file
}

// GetOptions returns the one-of's options. Most usages will be more interested
// in GetOneOfOptions, which has a concrete return type. This generic version
// is present to satisfy the Descriptor interface.
func (od *OneOfDescriptor) GetOptions() proto.Message {
	return od.proto.GetOptions()
}

// GetOneOfOptions returns the one-of's options.
func (od *OneOfDescriptor) GetOneOfOptions() *dpb.OneofOptions {
	return od.proto.GetOptions()
}

// GetSourceInfo returns source info for the one-of, if present in the
// descriptor. Not all descriptors will contain source info. If non-nil, the
// returned info contains information about the location in the file where the
// one-of was defined and also contains comments associated with the one-of
// definition.
func (od *OneOfDescriptor) GetSourceInfo() *dpb.SourceCodeInfo_Location {
	return od.file.sourceInfo.Get(od.sourceInfoPath)
}

// AsProto returns the underlying descriptor proto. Most usages will be more
// interested in AsOneofDescriptorProto, which has a concrete return type. This
// generic version is present to satisfy the Descriptor interface.
func (od *OneOfDescriptor) AsProto() proto.Message {
	return od.proto
}

// AsOneofDescriptorProto returns the underlying descriptor proto.
func (od *OneOfDescriptor) AsOneofDescriptorProto() *dpb.OneofDescriptorProto {
	return od.proto
}

// String returns the underlying descriptor proto, in compact text format.
func (od *OneOfDescriptor) String() string {
	return od.proto.String()
}

// GetChoices returns the fields that are part of the one-of field set. At most one of
// these fields may be set for a given message.
func (od *OneOfDescriptor) GetChoices() []*FieldDescriptor {
	return od.choices
}

func (od *OneOfDescriptor) IsSynthetic() bool {
	return len(od.choices) == 1 && od.choices[0].IsProto3Optional()
}

// scope represents a lexical scope in a proto file in which messages and enums
// can be declared.
type scope func(string) Descriptor

func fileScope(fd *FileDescriptor) scope {
	// we search symbols in this file, but also symbols in other files that have
	// the same package as this file or a "parent" package (in protobuf,
	// packages are a hierarchy like C++ namespaces)
	prefixes := internal.CreatePrefixList(fd.proto.GetPackage())
	return func(name string) Descriptor {
		for _, prefix := range prefixes {
			n := merge(prefix, name)
			d := findSymbol(fd, n, false)
			if d != nil {
				return d
			}
		}
		return nil
	}
}

func messageScope(md *MessageDescriptor) scope {
	return func(name string) Descriptor {
		n := merge(md.fqn, name)
		if d, ok := md.file.symbols[n]; ok {
			return d
		}
		return nil
	}
}

func resolve(fd *FileDescriptor, name string, scopes []scope) (Descriptor, error) {
	if strings.HasPrefix(name, ".") {
		// already fully-qualified
		d := findSymbol(fd, name[1:], false)
		if d != nil {
			return d, nil
		}
	} else {
		// unqualified, so we look in the enclosing (last) scope first and move
		// towards outermost (first) scope, trying to resolve the symbol
		for i := len(scopes) - 1; i >= 0; i-- {
			d := scopes[i](name)
			if d != nil {
				return d, nil
			}
		}
	}
	return nil, fmt.Errorf("file %q included an unresolvable reference to %q", fd.proto.GetName(), name)
}

func findSymbol(fd *FileDescriptor, name string, public bool) Descriptor {
	d := fd.symbols[name]
	if d != nil {
		return d
	}

	// When public = false, we are searching only directly imported symbols. But we
	// also need to search transitive public imports due to semantics of public imports.
	var deps []*FileDescriptor
	if public {
		deps = fd.publicDeps
	} else {
		deps = fd.deps
	}
	for _, dep := range deps {
		d = findSymbol(dep, name, true)
		if d != nil {
			return d
		}
	}

	return nil
}

func merge(a, b string) string {
	if a == "" {
		return b
	} else {
		return a + "." + b
	}
}
