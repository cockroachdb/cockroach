package protokit

import (
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/protoc-gen-go/plugin"

	"context"
	"fmt"
	"strings"
)

const (
	// tag numbers in FileDescriptorProto
	packageCommentPath   = 2
	messageCommentPath   = 4
	enumCommentPath      = 5
	serviceCommentPath   = 6
	extensionCommentPath = 7
	syntaxCommentPath    = 12

	// tag numbers in DescriptorProto
	messageFieldCommentPath     = 2 // field
	messageMessageCommentPath   = 3 // nested_type
	messageEnumCommentPath      = 4 // enum_type
	messageExtensionCommentPath = 6 // extension

	// tag numbers in EnumDescriptorProto
	enumValueCommentPath = 2 // value

	// tag numbers in ServiceDescriptorProto
	serviceMethodCommentPath = 2
)

// ParseCodeGenRequest parses the given request into `FileDescriptor` objects. Only the `req.FilesToGenerate` will be
// returned.
//
// For example, given the following invocation, only booking.proto will be returned even if it imports other protos:
//
//     protoc --plugin=protoc-gen-test=./test -I. protos/booking.proto
func ParseCodeGenRequest(req *plugin_go.CodeGeneratorRequest) []*FileDescriptor {
	allFiles := make(map[string]*FileDescriptor)
	genFiles := make([]*FileDescriptor, len(req.GetFileToGenerate()))

	for _, pf := range req.GetProtoFile() {
		allFiles[pf.GetName()] = parseFile(context.Background(), pf)
	}

	for i, f := range req.GetFileToGenerate() {
		genFiles[i] = allFiles[f]
		parseImports(genFiles[i], allFiles)
	}

	return genFiles
}

func parseFile(ctx context.Context, fd *descriptor.FileDescriptorProto) *FileDescriptor {
	comments := ParseComments(fd)

	file := &FileDescriptor{
		comments:            comments,
		FileDescriptorProto: fd,
		Comments:            comments.Get(fmt.Sprintf("%d", packageCommentPath)),
		PackageComments:     comments.Get(fmt.Sprintf("%d", packageCommentPath)),
		SyntaxComments:      comments.Get(fmt.Sprintf("%d", syntaxCommentPath)),
	}

	if fd.Options != nil {
		file.setOptions(fd.Options)
	}

	fileCtx := ContextWithFileDescriptor(ctx, file)
	file.Enums = parseEnums(fileCtx, fd.GetEnumType())
	file.Extensions = parseExtensions(fileCtx, fd.GetExtension())
	file.Messages = parseMessages(fileCtx, fd.GetMessageType())
	file.Services = parseServices(fileCtx, fd.GetService())

	return file
}

func parseEnums(ctx context.Context, protos []*descriptor.EnumDescriptorProto) []*EnumDescriptor {
	enums := make([]*EnumDescriptor, len(protos))
	file, _ := FileDescriptorFromContext(ctx)
	parent, hasParent := DescriptorFromContext(ctx)

	for i, ed := range protos {
		longName := ed.GetName()
		commentPath := fmt.Sprintf("%d.%d", enumCommentPath, i)

		if hasParent {
			longName = fmt.Sprintf("%s.%s", parent.GetLongName(), longName)
			commentPath = fmt.Sprintf("%s.%d.%d", parent.path, messageEnumCommentPath, i)
		}

		enums[i] = &EnumDescriptor{
			common:              newCommon(file, commentPath, longName),
			EnumDescriptorProto: ed,
			Comments:            file.comments.Get(commentPath),
			Parent:              parent,
		}
		if ed.Options != nil {
			enums[i].setOptions(ed.Options)
		}

		subCtx := ContextWithEnumDescriptor(ctx, enums[i])
		enums[i].Values = parseEnumValues(subCtx, ed.GetValue())
	}

	return enums
}

func parseEnumValues(ctx context.Context, protos []*descriptor.EnumValueDescriptorProto) []*EnumValueDescriptor {
	values := make([]*EnumValueDescriptor, len(protos))
	file, _ := FileDescriptorFromContext(ctx)
	enum, _ := EnumDescriptorFromContext(ctx)

	for i, vd := range protos {
		longName := fmt.Sprintf("%s.%s", enum.GetLongName(), vd.GetName())

		values[i] = &EnumValueDescriptor{
			common: newCommon(file, "", longName),
			EnumValueDescriptorProto: vd,
			Enum:     enum,
			Comments: file.comments.Get(fmt.Sprintf("%s.%d.%d", enum.path, enumValueCommentPath, i)),
		}
		if vd.Options != nil {
			values[i].setOptions(vd.Options)
		}
	}

	return values
}

func parseExtensions(ctx context.Context, protos []*descriptor.FieldDescriptorProto) []*ExtensionDescriptor {
	exts := make([]*ExtensionDescriptor, len(protos))
	file, _ := FileDescriptorFromContext(ctx)
	parent, hasParent := DescriptorFromContext(ctx)

	for i, ext := range protos {
		commentPath := fmt.Sprintf("%d.%d", extensionCommentPath, i)
		longName := fmt.Sprintf("%s.%s", ext.GetExtendee(), ext.GetName())

		if strings.Contains(longName, file.GetPackage()) {
			parts := strings.Split(ext.GetExtendee(), ".")
			longName = fmt.Sprintf("%s.%s", parts[len(parts)-1], ext.GetName())
		}

		if hasParent {
			commentPath = fmt.Sprintf("%s.%d.%d", parent.path, messageExtensionCommentPath, i)
		}

		exts[i] = &ExtensionDescriptor{
			common:               newCommon(file, commentPath, longName),
			FieldDescriptorProto: ext,
			Comments:             file.comments.Get(commentPath),
			Parent:               parent,
		}
		if ext.Options != nil {
			exts[i].setOptions(ext.Options)
		}
	}

	return exts
}

func parseImports(fd *FileDescriptor, allFiles map[string]*FileDescriptor) {
	fd.Imports = make([]*ImportedDescriptor, 0)

	for _, index := range fd.GetPublicDependency() {
		file := allFiles[fd.GetDependency()[index]]

		for _, d := range file.GetMessages() {
			// skip map entry objects
			if !d.GetOptions().GetMapEntry() {
				fd.Imports = append(fd.Imports, &ImportedDescriptor{d.common})
			}
		}

		for _, e := range file.GetEnums() {
			fd.Imports = append(fd.Imports, &ImportedDescriptor{e.common})
		}

		for _, ext := range file.GetExtensions() {
			fd.Imports = append(fd.Imports, &ImportedDescriptor{ext.common})
		}
	}
}

func parseMessages(ctx context.Context, protos []*descriptor.DescriptorProto) []*Descriptor {
	msgs := make([]*Descriptor, len(protos))
	file, _ := FileDescriptorFromContext(ctx)
	parent, hasParent := DescriptorFromContext(ctx)

	for i, md := range protos {
		longName := md.GetName()
		commentPath := fmt.Sprintf("%d.%d", messageCommentPath, i)

		if hasParent {
			longName = fmt.Sprintf("%s.%s", parent.GetLongName(), longName)
			commentPath = fmt.Sprintf("%s.%d.%d", parent.path, messageMessageCommentPath, i)
		}

		msgs[i] = &Descriptor{
			common:          newCommon(file, commentPath, longName),
			DescriptorProto: md,
			Comments:        file.comments.Get(commentPath),
			Parent:          parent,
		}
		if md.Options != nil {
			msgs[i].setOptions(md.Options)
		}

		msgCtx := ContextWithDescriptor(ctx, msgs[i])
		msgs[i].Enums = parseEnums(msgCtx, md.GetEnumType())
		msgs[i].Extensions = parseExtensions(msgCtx, md.GetExtension())
		msgs[i].Fields = parseMessageFields(msgCtx, md.GetField())
		msgs[i].Messages = parseMessages(msgCtx, md.GetNestedType())
	}

	return msgs
}

func parseMessageFields(ctx context.Context, protos []*descriptor.FieldDescriptorProto) []*FieldDescriptor {
	fields := make([]*FieldDescriptor, len(protos))
	file, _ := FileDescriptorFromContext(ctx)
	message, _ := DescriptorFromContext(ctx)

	for i, fd := range protos {
		longName := fmt.Sprintf("%s.%s", message.GetLongName(), fd.GetName())

		fields[i] = &FieldDescriptor{
			common:               newCommon(file, "", longName),
			FieldDescriptorProto: fd,
			Comments:             file.comments.Get(fmt.Sprintf("%s.%d.%d", message.path, messageFieldCommentPath, i)),
			Message:              message,
		}
		if fd.Options != nil {
			fields[i].setOptions(fd.Options)
		}
	}

	return fields
}

func parseServices(ctx context.Context, protos []*descriptor.ServiceDescriptorProto) []*ServiceDescriptor {
	svcs := make([]*ServiceDescriptor, len(protos))
	file, _ := FileDescriptorFromContext(ctx)

	for i, sd := range protos {
		longName := sd.GetName()
		commentPath := fmt.Sprintf("%d.%d", serviceCommentPath, i)

		svcs[i] = &ServiceDescriptor{
			common:                 newCommon(file, commentPath, longName),
			ServiceDescriptorProto: sd,
			Comments:               file.comments.Get(commentPath),
		}
		if sd.Options != nil {
			svcs[i].setOptions(sd.Options)
		}

		svcCtx := ContextWithServiceDescriptor(ctx, svcs[i])
		svcs[i].Methods = parseServiceMethods(svcCtx, sd.GetMethod())
	}

	return svcs
}

func parseServiceMethods(ctx context.Context, protos []*descriptor.MethodDescriptorProto) []*MethodDescriptor {
	methods := make([]*MethodDescriptor, len(protos))

	file, _ := FileDescriptorFromContext(ctx)
	svc, _ := ServiceDescriptorFromContext(ctx)

	for i, md := range protos {
		longName := fmt.Sprintf("%s.%s", svc.GetLongName(), md.GetName())

		methods[i] = &MethodDescriptor{
			common:                newCommon(file, "", longName),
			MethodDescriptorProto: md,
			Service:               svc,
			Comments:              file.comments.Get(fmt.Sprintf("%s.%d.%d", svc.path, serviceMethodCommentPath, i)),
		}
		if md.Options != nil {
			methods[i].setOptions(md.Options)
		}
	}

	return methods
}
