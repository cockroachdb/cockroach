package protokit

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"

	"fmt"
	"strings"
)

type common struct {
	file     *FileDescriptor
	path     string
	LongName string
	FullName string

	OptionExtensions map[string]interface{}
}

func newCommon(f *FileDescriptor, path, longName string) common {
	fn := longName
	if !strings.HasPrefix(fn, ".") {
		fn = fmt.Sprintf("%s.%s", f.GetPackage(), longName)
	}

	return common{
		file:     f,
		path:     path,
		LongName: longName,
		FullName: fn,
	}
}

// GetFile returns the FileDescriptor that contains this object
func (c *common) GetFile() *FileDescriptor { return c.file }

// GetPackage returns the package this object is in
func (c *common) GetPackage() string { return c.file.GetPackage() }

// GetLongName returns the name prefixed with the dot-separated parent descriptor's name (if any)
func (c *common) GetLongName() string { return c.LongName }

// GetFullName returns the `LongName` prefixed with the package this object is in
func (c *common) GetFullName() string { return c.FullName }

// IsProto3 returns whether or not this is a proto3 object
func (c *common) IsProto3() bool { return c.file.GetSyntax() == "proto3" }

func getOptions(options proto.Message) (m map[string]interface{}) {
	for _, extension := range proto.RegisteredExtensions(options) {
		if !proto.HasExtension(options, extension) {
			continue
		}
		ext, err := proto.GetExtension(options, extension)
		if err != nil {
			continue
		}
		if m == nil {
			m = make(map[string]interface{})
		}
		m[extension.Name] = ext
	}
	return m
}

func (c *common) setOptions(options proto.Message) {
	if opts := getOptions(options); len(opts) > 0 {
		if c.OptionExtensions == nil {
			c.OptionExtensions = opts
			return
		}
		for k, v := range opts {
			c.OptionExtensions[k] = v
		}
	}
}

// An ImportedDescriptor describes a type that was imported by a FileDescriptor.
type ImportedDescriptor struct {
	common
}

// A FileDescriptor describes a single proto file with all of its messages, enums, services, etc.
type FileDescriptor struct {
	comments Comments
	*descriptor.FileDescriptorProto

	Comments        *Comment // Deprecated: see PackageComments
	PackageComments *Comment
	SyntaxComments  *Comment

	Enums      []*EnumDescriptor
	Extensions []*ExtensionDescriptor
	Imports    []*ImportedDescriptor
	Messages   []*Descriptor
	Services   []*ServiceDescriptor

	OptionExtensions map[string]interface{}
}

// IsProto3 returns whether or not this file is a proto3 file
func (f *FileDescriptor) IsProto3() bool { return f.GetSyntax() == "proto3" }

// GetComments returns the file's package comments.
//
// Deprecated: please see GetPackageComments
func (f *FileDescriptor) GetComments() *Comment { return f.Comments }

// GetPackageComments returns the file's package comments
func (f *FileDescriptor) GetPackageComments() *Comment { return f.PackageComments }

// GetSyntaxComments returns the file's syntax comments
func (f *FileDescriptor) GetSyntaxComments() *Comment { return f.SyntaxComments }

// GetEnums returns the top-level enumerations defined in this file
func (f *FileDescriptor) GetEnums() []*EnumDescriptor { return f.Enums }

// GetExtensions returns the top-level (file) extensions defined in this file
func (f *FileDescriptor) GetExtensions() []*ExtensionDescriptor { return f.Extensions }

// GetImports returns the proto files imported by this file
func (f *FileDescriptor) GetImports() []*ImportedDescriptor { return f.Imports }

// GetMessages returns the top-level messages defined in this file
func (f *FileDescriptor) GetMessages() []*Descriptor { return f.Messages }

// GetServices returns the services defined in this file
func (f *FileDescriptor) GetServices() []*ServiceDescriptor { return f.Services }

// GetEnum returns the enumeration with the specified name (returns `nil` if not found)
func (f *FileDescriptor) GetEnum(name string) *EnumDescriptor {
	for _, e := range f.GetEnums() {
		if e.GetName() == name || e.GetLongName() == name {
			return e
		}
	}

	return nil
}

// GetMessage returns the message with the specified name (returns `nil` if not found)
func (f *FileDescriptor) GetMessage(name string) *Descriptor {
	for _, m := range f.GetMessages() {
		if m.GetName() == name || m.GetLongName() == name {
			return m
		}
	}

	return nil
}

// GetService returns the service with the specified name (returns `nil` if not found)
func (f *FileDescriptor) GetService(name string) *ServiceDescriptor {
	for _, s := range f.GetServices() {
		if s.GetName() == name || s.GetLongName() == name {
			return s
		}
	}

	return nil
}

func (f *FileDescriptor) setOptions(options proto.Message) {
	if opts := getOptions(options); len(opts) > 0 {
		if f.OptionExtensions == nil {
			f.OptionExtensions = opts
			return
		}
		for k, v := range opts {
			f.OptionExtensions[k] = v
		}
	}
}

// An EnumDescriptor describe an enum type
type EnumDescriptor struct {
	common
	*descriptor.EnumDescriptorProto
	Parent   *Descriptor
	Values   []*EnumValueDescriptor
	Comments *Comment
}

// GetComments returns a description of this enum
func (e *EnumDescriptor) GetComments() *Comment { return e.Comments }

// GetParent returns the parent message (if any) that contains this enum
func (e *EnumDescriptor) GetParent() *Descriptor { return e.Parent }

// GetValues returns the available values for this enum
func (e *EnumDescriptor) GetValues() []*EnumValueDescriptor { return e.Values }

// GetNamedValue returns the value with the specified name (returns `nil` if not found)
func (e *EnumDescriptor) GetNamedValue(name string) *EnumValueDescriptor {
	for _, v := range e.GetValues() {
		if v.GetName() == name {
			return v
		}
	}

	return nil
}

// An EnumValueDescriptor describes an enum value
type EnumValueDescriptor struct {
	common
	*descriptor.EnumValueDescriptorProto
	Enum     *EnumDescriptor
	Comments *Comment
}

// GetComments returns a description of the value
func (v *EnumValueDescriptor) GetComments() *Comment { return v.Comments }

// GetEnum returns the parent enumeration that contains this value
func (v *EnumValueDescriptor) GetEnum() *EnumDescriptor { return v.Enum }

// An ExtensionDescriptor describes a protobuf extension. If it's a top-level extension it's parent will be `nil`
type ExtensionDescriptor struct {
	common
	*descriptor.FieldDescriptorProto
	Parent   *Descriptor
	Comments *Comment
}

// GetComments returns a description of the extension
func (e *ExtensionDescriptor) GetComments() *Comment { return e.Comments }

// GetParent returns the descriptor that defined this extension (if any)
func (e *ExtensionDescriptor) GetParent() *Descriptor { return e.Parent }

// A Descriptor describes a message
type Descriptor struct {
	common
	*descriptor.DescriptorProto
	Parent     *Descriptor
	Comments   *Comment
	Enums      []*EnumDescriptor
	Extensions []*ExtensionDescriptor
	Fields     []*FieldDescriptor
	Messages   []*Descriptor
}

// GetComments returns a description of the message
func (m *Descriptor) GetComments() *Comment { return m.Comments }

// GetParent returns the parent descriptor (if any) that defines this descriptor
func (m *Descriptor) GetParent() *Descriptor { return m.Parent }

// GetEnums returns the nested enumerations within the message
func (m *Descriptor) GetEnums() []*EnumDescriptor { return m.Enums }

// GetExtensions returns the message-level extensions defined by this message
func (m *Descriptor) GetExtensions() []*ExtensionDescriptor { return m.Extensions }

// GetMessages returns the nested messages within the message
func (m *Descriptor) GetMessages() []*Descriptor { return m.Messages }

// GetMessageFields returns the message fields
func (m *Descriptor) GetMessageFields() []*FieldDescriptor { return m.Fields }

// GetEnum returns the enum with the specified name. The name can be either simple, or fully qualified (returns `nil` if
// not found)
func (m *Descriptor) GetEnum(name string) *EnumDescriptor {
	for _, e := range m.GetEnums() {
		// can lookup by name or message prefixed name (qualified)
		if e.GetName() == name || e.GetLongName() == name {
			return e
		}
	}

	return nil
}

// GetMessage returns the nested message with the specified name. The name can be simple or fully qualified (returns
// `nil` if not found)
func (m *Descriptor) GetMessage(name string) *Descriptor {
	for _, msg := range m.GetMessages() {
		// can lookup by name or message prefixed name (qualified)
		if msg.GetName() == name || msg.GetLongName() == name {
			return msg
		}
	}

	return nil
}

// GetMessageField returns the field with the specified name (returns `nil` if not found)
func (m *Descriptor) GetMessageField(name string) *FieldDescriptor {
	for _, f := range m.GetMessageFields() {
		if f.GetName() == name || f.GetLongName() == name {
			return f
		}
	}

	return nil
}

// A FieldDescriptor describes a message field
type FieldDescriptor struct {
	common
	*descriptor.FieldDescriptorProto
	Comments *Comment
	Message  *Descriptor
}

// GetComments returns a description of the field
func (mf *FieldDescriptor) GetComments() *Comment { return mf.Comments }

// GetMessage returns the descriptor that defines this field
func (mf *FieldDescriptor) GetMessage() *Descriptor { return mf.Message }

// A ServiceDescriptor describes a service
type ServiceDescriptor struct {
	common
	*descriptor.ServiceDescriptorProto
	Comments *Comment
	Methods  []*MethodDescriptor
}

// GetComments returns a description of the service
func (s *ServiceDescriptor) GetComments() *Comment { return s.Comments }

// GetMethods returns the methods for the service
func (s *ServiceDescriptor) GetMethods() []*MethodDescriptor { return s.Methods }

// GetNamedMethod returns the method with the specified name (if found)
func (s *ServiceDescriptor) GetNamedMethod(name string) *MethodDescriptor {
	for _, m := range s.GetMethods() {
		if m.GetName() == name || m.GetLongName() == name {
			return m
		}
	}

	return nil
}

// A MethodDescriptor describes a method in a service
type MethodDescriptor struct {
	common
	*descriptor.MethodDescriptorProto
	Comments *Comment
	Service  *ServiceDescriptor
}

// GetComments returns a description of the method
func (m *MethodDescriptor) GetComments() *Comment { return m.Comments }

// GetService returns the service descriptor that defines this method
func (m *MethodDescriptor) GetService() *ServiceDescriptor { return m.Service }
