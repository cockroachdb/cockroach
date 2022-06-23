package protoparse

import (
	"bytes"
	"fmt"
	"math"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/internal"
	"github.com/jhump/protoreflect/desc/protoparse/ast"
	"github.com/jhump/protoreflect/dynamic"
)

// NB: To process options, we need descriptors, but we may not have rich
// descriptors when trying to interpret options for unlinked parsed files.
// So we define minimal interfaces that can be backed by both rich descriptors
// as well as their poorer cousins, plain ol' descriptor protos.

type descriptorish interface {
	GetFile() fileDescriptorish
	GetFullyQualifiedName() string
	AsProto() proto.Message
}

type fileDescriptorish interface {
	descriptorish
	GetFileOptions() *dpb.FileOptions
	GetPackage() string
	FindSymbol(name string) desc.Descriptor
	GetPublicDependencies() []fileDescriptorish
	GetDependencies() []fileDescriptorish
	GetMessageTypes() []msgDescriptorish
	GetExtensions() []fldDescriptorish
	GetEnumTypes() []enumDescriptorish
	GetServices() []svcDescriptorish
}

type msgDescriptorish interface {
	descriptorish
	GetMessageOptions() *dpb.MessageOptions
	GetFields() []fldDescriptorish
	GetOneOfs() []oneofDescriptorish
	GetExtensionRanges() []extRangeDescriptorish
	GetNestedMessageTypes() []msgDescriptorish
	GetNestedExtensions() []fldDescriptorish
	GetNestedEnumTypes() []enumDescriptorish
}

type fldDescriptorish interface {
	descriptorish
	GetFieldOptions() *dpb.FieldOptions
	GetMessageType() *desc.MessageDescriptor
	GetEnumType() *desc.EnumDescriptor
	AsFieldDescriptorProto() *dpb.FieldDescriptorProto
}

type oneofDescriptorish interface {
	descriptorish
	GetOneOfOptions() *dpb.OneofOptions
}

type enumDescriptorish interface {
	descriptorish
	GetEnumOptions() *dpb.EnumOptions
	GetValues() []enumValDescriptorish
}

type enumValDescriptorish interface {
	descriptorish
	GetEnumValueOptions() *dpb.EnumValueOptions
}

type svcDescriptorish interface {
	descriptorish
	GetServiceOptions() *dpb.ServiceOptions
	GetMethods() []methodDescriptorish
}

type methodDescriptorish interface {
	descriptorish
	GetMethodOptions() *dpb.MethodOptions
}

// The hierarchy of descriptorish implementations backed by
// rich descriptors:

type richFileDescriptorish struct {
	*desc.FileDescriptor
}

func (d richFileDescriptorish) GetFile() fileDescriptorish {
	return d
}

func (d richFileDescriptorish) GetPublicDependencies() []fileDescriptorish {
	deps := d.FileDescriptor.GetPublicDependencies()
	ret := make([]fileDescriptorish, len(deps))
	for i, d := range deps {
		ret[i] = richFileDescriptorish{FileDescriptor: d}
	}
	return ret
}

func (d richFileDescriptorish) GetDependencies() []fileDescriptorish {
	deps := d.FileDescriptor.GetDependencies()
	ret := make([]fileDescriptorish, len(deps))
	for i, d := range deps {
		ret[i] = richFileDescriptorish{FileDescriptor: d}
	}
	return ret
}

func (d richFileDescriptorish) GetMessageTypes() []msgDescriptorish {
	msgs := d.FileDescriptor.GetMessageTypes()
	ret := make([]msgDescriptorish, len(msgs))
	for i, m := range msgs {
		ret[i] = richMsgDescriptorish{MessageDescriptor: m}
	}
	return ret
}

func (d richFileDescriptorish) GetExtensions() []fldDescriptorish {
	flds := d.FileDescriptor.GetExtensions()
	ret := make([]fldDescriptorish, len(flds))
	for i, f := range flds {
		ret[i] = richFldDescriptorish{FieldDescriptor: f}
	}
	return ret
}

func (d richFileDescriptorish) GetEnumTypes() []enumDescriptorish {
	ens := d.FileDescriptor.GetEnumTypes()
	ret := make([]enumDescriptorish, len(ens))
	for i, en := range ens {
		ret[i] = richEnumDescriptorish{EnumDescriptor: en}
	}
	return ret
}

func (d richFileDescriptorish) GetServices() []svcDescriptorish {
	svcs := d.FileDescriptor.GetServices()
	ret := make([]svcDescriptorish, len(svcs))
	for i, s := range svcs {
		ret[i] = richSvcDescriptorish{ServiceDescriptor: s}
	}
	return ret
}

type richMsgDescriptorish struct {
	*desc.MessageDescriptor
}

func (d richMsgDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.MessageDescriptor.GetFile()}
}

func (d richMsgDescriptorish) GetFields() []fldDescriptorish {
	flds := d.MessageDescriptor.GetFields()
	ret := make([]fldDescriptorish, len(flds))
	for i, f := range flds {
		ret[i] = richFldDescriptorish{FieldDescriptor: f}
	}
	return ret
}

func (d richMsgDescriptorish) GetOneOfs() []oneofDescriptorish {
	oos := d.MessageDescriptor.GetOneOfs()
	ret := make([]oneofDescriptorish, len(oos))
	for i, oo := range oos {
		ret[i] = richOneOfDescriptorish{OneOfDescriptor: oo}
	}
	return ret
}

func (d richMsgDescriptorish) GetExtensionRanges() []extRangeDescriptorish {
	md := d.MessageDescriptor
	mdFqn := md.GetFullyQualifiedName()
	extrs := md.AsDescriptorProto().GetExtensionRange()
	ret := make([]extRangeDescriptorish, len(extrs))
	for i, extr := range extrs {
		ret[i] = extRangeDescriptorish{
			er:   extr,
			qual: mdFqn,
			file: richFileDescriptorish{FileDescriptor: md.GetFile()},
		}
	}
	return ret
}

func (d richMsgDescriptorish) GetNestedMessageTypes() []msgDescriptorish {
	msgs := d.MessageDescriptor.GetNestedMessageTypes()
	ret := make([]msgDescriptorish, len(msgs))
	for i, m := range msgs {
		ret[i] = richMsgDescriptorish{MessageDescriptor: m}
	}
	return ret
}

func (d richMsgDescriptorish) GetNestedExtensions() []fldDescriptorish {
	flds := d.MessageDescriptor.GetNestedExtensions()
	ret := make([]fldDescriptorish, len(flds))
	for i, f := range flds {
		ret[i] = richFldDescriptorish{FieldDescriptor: f}
	}
	return ret
}

func (d richMsgDescriptorish) GetNestedEnumTypes() []enumDescriptorish {
	ens := d.MessageDescriptor.GetNestedEnumTypes()
	ret := make([]enumDescriptorish, len(ens))
	for i, en := range ens {
		ret[i] = richEnumDescriptorish{EnumDescriptor: en}
	}
	return ret
}

type richFldDescriptorish struct {
	*desc.FieldDescriptor
}

func (d richFldDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.FieldDescriptor.GetFile()}
}

func (d richFldDescriptorish) AsFieldDescriptorProto() *dpb.FieldDescriptorProto {
	return d.FieldDescriptor.AsFieldDescriptorProto()
}

type richOneOfDescriptorish struct {
	*desc.OneOfDescriptor
}

func (d richOneOfDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.OneOfDescriptor.GetFile()}
}

type richEnumDescriptorish struct {
	*desc.EnumDescriptor
}

func (d richEnumDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.EnumDescriptor.GetFile()}
}

func (d richEnumDescriptorish) GetValues() []enumValDescriptorish {
	vals := d.EnumDescriptor.GetValues()
	ret := make([]enumValDescriptorish, len(vals))
	for i, val := range vals {
		ret[i] = richEnumValDescriptorish{EnumValueDescriptor: val}
	}
	return ret
}

type richEnumValDescriptorish struct {
	*desc.EnumValueDescriptor
}

func (d richEnumValDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.EnumValueDescriptor.GetFile()}
}

type richSvcDescriptorish struct {
	*desc.ServiceDescriptor
}

func (d richSvcDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.ServiceDescriptor.GetFile()}
}

func (d richSvcDescriptorish) GetMethods() []methodDescriptorish {
	mtds := d.ServiceDescriptor.GetMethods()
	ret := make([]methodDescriptorish, len(mtds))
	for i, mtd := range mtds {
		ret[i] = richMethodDescriptorish{MethodDescriptor: mtd}
	}
	return ret
}

type richMethodDescriptorish struct {
	*desc.MethodDescriptor
}

func (d richMethodDescriptorish) GetFile() fileDescriptorish {
	return richFileDescriptorish{FileDescriptor: d.MethodDescriptor.GetFile()}
}

// The hierarchy of descriptorish implementations backed by
// plain descriptor protos:

type poorFileDescriptorish struct {
	*dpb.FileDescriptorProto
}

func (d poorFileDescriptorish) GetFile() fileDescriptorish {
	return d
}

func (d poorFileDescriptorish) GetFullyQualifiedName() string {
	return d.FileDescriptorProto.GetName()
}

func (d poorFileDescriptorish) AsProto() proto.Message {
	return d.FileDescriptorProto
}

func (d poorFileDescriptorish) GetFileOptions() *dpb.FileOptions {
	return d.FileDescriptorProto.GetOptions()
}

func (d poorFileDescriptorish) FindSymbol(name string) desc.Descriptor {
	return nil
}

func (d poorFileDescriptorish) GetPublicDependencies() []fileDescriptorish {
	return nil
}

func (d poorFileDescriptorish) GetDependencies() []fileDescriptorish {
	return nil
}

func (d poorFileDescriptorish) GetMessageTypes() []msgDescriptorish {
	msgs := d.FileDescriptorProto.GetMessageType()
	pkg := d.FileDescriptorProto.GetPackage()
	ret := make([]msgDescriptorish, len(msgs))
	for i, m := range msgs {
		ret[i] = poorMsgDescriptorish{
			DescriptorProto: m,
			qual:            pkg,
			file:            d,
		}
	}
	return ret
}

func (d poorFileDescriptorish) GetExtensions() []fldDescriptorish {
	exts := d.FileDescriptorProto.GetExtension()
	pkg := d.FileDescriptorProto.GetPackage()
	ret := make([]fldDescriptorish, len(exts))
	for i, e := range exts {
		ret[i] = poorFldDescriptorish{
			FieldDescriptorProto: e,
			qual:                 pkg,
			file:                 d,
		}
	}
	return ret
}

func (d poorFileDescriptorish) GetEnumTypes() []enumDescriptorish {
	ens := d.FileDescriptorProto.GetEnumType()
	pkg := d.FileDescriptorProto.GetPackage()
	ret := make([]enumDescriptorish, len(ens))
	for i, e := range ens {
		ret[i] = poorEnumDescriptorish{
			EnumDescriptorProto: e,
			qual:                pkg,
			file:                d,
		}
	}
	return ret
}

func (d poorFileDescriptorish) GetServices() []svcDescriptorish {
	svcs := d.FileDescriptorProto.GetService()
	pkg := d.FileDescriptorProto.GetPackage()
	ret := make([]svcDescriptorish, len(svcs))
	for i, s := range svcs {
		ret[i] = poorSvcDescriptorish{
			ServiceDescriptorProto: s,
			qual:                   pkg,
			file:                   d,
		}
	}
	return ret
}

type poorMsgDescriptorish struct {
	*dpb.DescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorMsgDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorMsgDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.DescriptorProto.GetName())
}

func qualify(qual, name string) string {
	if qual == "" {
		return name
	} else {
		return fmt.Sprintf("%s.%s", qual, name)
	}
}

func (d poorMsgDescriptorish) AsProto() proto.Message {
	return d.DescriptorProto
}

func (d poorMsgDescriptorish) GetMessageOptions() *dpb.MessageOptions {
	return d.DescriptorProto.GetOptions()
}

func (d poorMsgDescriptorish) GetFields() []fldDescriptorish {
	flds := d.DescriptorProto.GetField()
	ret := make([]fldDescriptorish, len(flds))
	for i, f := range flds {
		ret[i] = poorFldDescriptorish{
			FieldDescriptorProto: f,
			qual:                 d.GetFullyQualifiedName(),
			file:                 d.file,
		}
	}
	return ret
}

func (d poorMsgDescriptorish) GetOneOfs() []oneofDescriptorish {
	oos := d.DescriptorProto.GetOneofDecl()
	ret := make([]oneofDescriptorish, len(oos))
	for i, oo := range oos {
		ret[i] = poorOneOfDescriptorish{
			OneofDescriptorProto: oo,
			qual:                 d.GetFullyQualifiedName(),
			file:                 d.file,
		}
	}
	return ret
}

func (d poorMsgDescriptorish) GetExtensionRanges() []extRangeDescriptorish {
	mdFqn := d.GetFullyQualifiedName()
	extrs := d.DescriptorProto.GetExtensionRange()
	ret := make([]extRangeDescriptorish, len(extrs))
	for i, extr := range extrs {
		ret[i] = extRangeDescriptorish{
			er:   extr,
			qual: mdFqn,
			file: d.file,
		}
	}
	return ret
}

func (d poorMsgDescriptorish) GetNestedMessageTypes() []msgDescriptorish {
	msgs := d.DescriptorProto.GetNestedType()
	ret := make([]msgDescriptorish, len(msgs))
	for i, m := range msgs {
		ret[i] = poorMsgDescriptorish{
			DescriptorProto: m,
			qual:            d.GetFullyQualifiedName(),
			file:            d.file,
		}
	}
	return ret
}

func (d poorMsgDescriptorish) GetNestedExtensions() []fldDescriptorish {
	flds := d.DescriptorProto.GetExtension()
	ret := make([]fldDescriptorish, len(flds))
	for i, f := range flds {
		ret[i] = poorFldDescriptorish{
			FieldDescriptorProto: f,
			qual:                 d.GetFullyQualifiedName(),
			file:                 d.file,
		}
	}
	return ret
}

func (d poorMsgDescriptorish) GetNestedEnumTypes() []enumDescriptorish {
	ens := d.DescriptorProto.GetEnumType()
	ret := make([]enumDescriptorish, len(ens))
	for i, en := range ens {
		ret[i] = poorEnumDescriptorish{
			EnumDescriptorProto: en,
			qual:                d.GetFullyQualifiedName(),
			file:                d.file,
		}
	}
	return ret
}

type poorFldDescriptorish struct {
	*dpb.FieldDescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorFldDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorFldDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.FieldDescriptorProto.GetName())
}

func (d poorFldDescriptorish) AsProto() proto.Message {
	return d.FieldDescriptorProto
}

func (d poorFldDescriptorish) GetFieldOptions() *dpb.FieldOptions {
	return d.FieldDescriptorProto.GetOptions()
}

func (d poorFldDescriptorish) GetMessageType() *desc.MessageDescriptor {
	return nil
}

func (d poorFldDescriptorish) GetEnumType() *desc.EnumDescriptor {
	return nil
}

type poorOneOfDescriptorish struct {
	*dpb.OneofDescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorOneOfDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorOneOfDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.OneofDescriptorProto.GetName())
}

func (d poorOneOfDescriptorish) AsProto() proto.Message {
	return d.OneofDescriptorProto
}

func (d poorOneOfDescriptorish) GetOneOfOptions() *dpb.OneofOptions {
	return d.OneofDescriptorProto.GetOptions()
}

func (d poorFldDescriptorish) AsFieldDescriptorProto() *dpb.FieldDescriptorProto {
	return d.FieldDescriptorProto
}

type poorEnumDescriptorish struct {
	*dpb.EnumDescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorEnumDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorEnumDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.EnumDescriptorProto.GetName())
}

func (d poorEnumDescriptorish) AsProto() proto.Message {
	return d.EnumDescriptorProto
}

func (d poorEnumDescriptorish) GetEnumOptions() *dpb.EnumOptions {
	return d.EnumDescriptorProto.GetOptions()
}

func (d poorEnumDescriptorish) GetValues() []enumValDescriptorish {
	vals := d.EnumDescriptorProto.GetValue()
	ret := make([]enumValDescriptorish, len(vals))
	for i, v := range vals {
		ret[i] = poorEnumValDescriptorish{
			EnumValueDescriptorProto: v,
			qual:                     d.GetFullyQualifiedName(),
			file:                     d.file,
		}
	}
	return ret
}

type poorEnumValDescriptorish struct {
	*dpb.EnumValueDescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorEnumValDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorEnumValDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.EnumValueDescriptorProto.GetName())
}

func (d poorEnumValDescriptorish) AsProto() proto.Message {
	return d.EnumValueDescriptorProto
}

func (d poorEnumValDescriptorish) GetEnumValueOptions() *dpb.EnumValueOptions {
	return d.EnumValueDescriptorProto.GetOptions()
}

type poorSvcDescriptorish struct {
	*dpb.ServiceDescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorSvcDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorSvcDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.ServiceDescriptorProto.GetName())
}

func (d poorSvcDescriptorish) AsProto() proto.Message {
	return d.ServiceDescriptorProto
}

func (d poorSvcDescriptorish) GetServiceOptions() *dpb.ServiceOptions {
	return d.ServiceDescriptorProto.GetOptions()
}

func (d poorSvcDescriptorish) GetMethods() []methodDescriptorish {
	mtds := d.ServiceDescriptorProto.GetMethod()
	ret := make([]methodDescriptorish, len(mtds))
	for i, m := range mtds {
		ret[i] = poorMethodDescriptorish{
			MethodDescriptorProto: m,
			qual:                  d.GetFullyQualifiedName(),
			file:                  d.file,
		}
	}
	return ret
}

type poorMethodDescriptorish struct {
	*dpb.MethodDescriptorProto
	qual string
	file fileDescriptorish
}

func (d poorMethodDescriptorish) GetFile() fileDescriptorish {
	return d.file
}

func (d poorMethodDescriptorish) GetFullyQualifiedName() string {
	return qualify(d.qual, d.MethodDescriptorProto.GetName())
}

func (d poorMethodDescriptorish) AsProto() proto.Message {
	return d.MethodDescriptorProto
}

func (d poorMethodDescriptorish) GetMethodOptions() *dpb.MethodOptions {
	return d.MethodDescriptorProto.GetOptions()
}

type extRangeDescriptorish struct {
	er   *dpb.DescriptorProto_ExtensionRange
	qual string
	file fileDescriptorish
}

func (er extRangeDescriptorish) GetFile() fileDescriptorish {
	return er.file
}

func (er extRangeDescriptorish) GetFullyQualifiedName() string {
	return qualify(er.qual, fmt.Sprintf("%d-%d", er.er.GetStart(), er.er.GetEnd()-1))
}

func (er extRangeDescriptorish) AsProto() proto.Message {
	return er.er
}

func (er extRangeDescriptorish) GetExtensionRangeOptions() *dpb.ExtensionRangeOptions {
	return er.er.GetOptions()
}

func interpretFileOptions(l *linker, r *parseResult, fd fileDescriptorish) error {
	opts := fd.GetFileOptions()
	if opts != nil {
		if len(opts.UninterpretedOption) > 0 {
			if remain, err := interpretOptions(l, r, fd, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
	}
	for _, md := range fd.GetMessageTypes() {
		if err := interpretMessageOptions(l, r, md); err != nil {
			return err
		}
	}
	for _, fld := range fd.GetExtensions() {
		if err := interpretFieldOptions(l, r, fld); err != nil {
			return err
		}
	}
	for _, ed := range fd.GetEnumTypes() {
		if err := interpretEnumOptions(l, r, ed); err != nil {
			return err
		}
	}
	for _, sd := range fd.GetServices() {
		opts := sd.GetServiceOptions()
		if len(opts.GetUninterpretedOption()) > 0 {
			if remain, err := interpretOptions(l, r, sd, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
		for _, mtd := range sd.GetMethods() {
			opts := mtd.GetMethodOptions()
			if len(opts.GetUninterpretedOption()) > 0 {
				if remain, err := interpretOptions(l, r, mtd, opts, opts.UninterpretedOption); err != nil {
					return err
				} else {
					opts.UninterpretedOption = remain
				}
			}
		}
	}
	return nil
}

func interpretMessageOptions(l *linker, r *parseResult, md msgDescriptorish) error {
	opts := md.GetMessageOptions()
	if opts != nil {
		if len(opts.UninterpretedOption) > 0 {
			if remain, err := interpretOptions(l, r, md, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
	}
	for _, fld := range md.GetFields() {
		if err := interpretFieldOptions(l, r, fld); err != nil {
			return err
		}
	}
	for _, ood := range md.GetOneOfs() {
		opts := ood.GetOneOfOptions()
		if len(opts.GetUninterpretedOption()) > 0 {
			if remain, err := interpretOptions(l, r, ood, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
	}
	for _, fld := range md.GetNestedExtensions() {
		if err := interpretFieldOptions(l, r, fld); err != nil {
			return err
		}
	}
	for _, er := range md.GetExtensionRanges() {
		opts := er.GetExtensionRangeOptions()
		if len(opts.GetUninterpretedOption()) > 0 {
			if remain, err := interpretOptions(l, r, er, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
	}
	for _, nmd := range md.GetNestedMessageTypes() {
		if err := interpretMessageOptions(l, r, nmd); err != nil {
			return err
		}
	}
	for _, ed := range md.GetNestedEnumTypes() {
		if err := interpretEnumOptions(l, r, ed); err != nil {
			return err
		}
	}
	return nil
}

func interpretFieldOptions(l *linker, r *parseResult, fld fldDescriptorish) error {
	opts := fld.GetFieldOptions()
	if len(opts.GetUninterpretedOption()) > 0 {
		uo := opts.UninterpretedOption
		scope := fmt.Sprintf("field %s", fld.GetFullyQualifiedName())

		// process json_name pseudo-option
		if index, err := findOption(r, scope, uo, "json_name"); err != nil && !r.lenient {
			return err
		} else if index >= 0 {
			opt := uo[index]
			optNode := r.getOptionNode(opt)

			// attribute source code info
			if on, ok := optNode.(*ast.OptionNode); ok {
				r.interpretedOptions[on] = []int32{-1, internal.Field_jsonNameTag}
			}
			uo = removeOption(uo, index)
			if opt.StringValue == nil {
				if err := r.errs.handleErrorWithPos(optNode.GetValue().Start(), "%s: expecting string value for json_name option", scope); err != nil {
					return err
				}
			} else {
				fld.AsFieldDescriptorProto().JsonName = proto.String(string(opt.StringValue))
			}
		}

		// and process default pseudo-option
		if index, err := processDefaultOption(r, scope, fld, uo); err != nil && !r.lenient {
			return err
		} else if index >= 0 {
			// attribute source code info
			optNode := r.getOptionNode(uo[index])
			if on, ok := optNode.(*ast.OptionNode); ok {
				r.interpretedOptions[on] = []int32{-1, internal.Field_defaultTag}
			}
			uo = removeOption(uo, index)
		}

		if len(uo) == 0 {
			// no real options, only pseudo-options above? clear out options
			fld.AsFieldDescriptorProto().Options = nil
		} else if remain, err := interpretOptions(l, r, fld, opts, uo); err != nil {
			return err
		} else {
			opts.UninterpretedOption = remain
		}
	}
	return nil
}

func processDefaultOption(res *parseResult, scope string, fld fldDescriptorish, uos []*dpb.UninterpretedOption) (defaultIndex int, err error) {
	found, err := findOption(res, scope, uos, "default")
	if err != nil || found == -1 {
		return -1, err
	}
	opt := uos[found]
	optNode := res.getOptionNode(opt)
	fdp := fld.AsFieldDescriptorProto()
	if fdp.GetLabel() == dpb.FieldDescriptorProto_LABEL_REPEATED {
		return -1, res.errs.handleErrorWithPos(optNode.GetName().Start(), "%s: default value cannot be set because field is repeated", scope)
	}
	if fdp.GetType() == dpb.FieldDescriptorProto_TYPE_GROUP || fdp.GetType() == dpb.FieldDescriptorProto_TYPE_MESSAGE {
		return -1, res.errs.handleErrorWithPos(optNode.GetName().Start(), "%s: default value cannot be set because field is a message", scope)
	}
	val := optNode.GetValue()
	if _, ok := val.(*ast.MessageLiteralNode); ok {
		return -1, res.errs.handleErrorWithPos(val.Start(), "%s: default value cannot be a message", scope)
	}
	mc := &messageContext{
		res:         res,
		file:        fld.GetFile(),
		elementName: fld.GetFullyQualifiedName(),
		elementType: descriptorType(fld.AsProto()),
		option:      opt,
	}
	v, err := fieldValue(res, mc, fld, val, true)
	if err != nil {
		return -1, res.errs.handleError(err)
	}
	if str, ok := v.(string); ok {
		fld.AsFieldDescriptorProto().DefaultValue = proto.String(str)
	} else if b, ok := v.([]byte); ok {
		fld.AsFieldDescriptorProto().DefaultValue = proto.String(encodeDefaultBytes(b))
	} else {
		var flt float64
		var ok bool
		if flt, ok = v.(float64); !ok {
			var flt32 float32
			if flt32, ok = v.(float32); ok {
				flt = float64(flt32)
			}
		}
		if ok {
			if math.IsInf(flt, 1) {
				fld.AsFieldDescriptorProto().DefaultValue = proto.String("inf")
			} else if ok && math.IsInf(flt, -1) {
				fld.AsFieldDescriptorProto().DefaultValue = proto.String("-inf")
			} else if ok && math.IsNaN(flt) {
				fld.AsFieldDescriptorProto().DefaultValue = proto.String("nan")
			} else {
				fld.AsFieldDescriptorProto().DefaultValue = proto.String(fmt.Sprintf("%v", v))
			}
		} else {
			fld.AsFieldDescriptorProto().DefaultValue = proto.String(fmt.Sprintf("%v", v))
		}
	}
	return found, nil
}

func encodeDefaultBytes(b []byte) string {
	var buf bytes.Buffer
	writeEscapedBytes(&buf, b)
	return buf.String()
}

func interpretEnumOptions(l *linker, r *parseResult, ed enumDescriptorish) error {
	opts := ed.GetEnumOptions()
	if opts != nil {
		if len(opts.UninterpretedOption) > 0 {
			if remain, err := interpretOptions(l, r, ed, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
	}
	for _, evd := range ed.GetValues() {
		opts := evd.GetEnumValueOptions()
		if len(opts.GetUninterpretedOption()) > 0 {
			if remain, err := interpretOptions(l, r, evd, opts, opts.UninterpretedOption); err != nil {
				return err
			} else {
				opts.UninterpretedOption = remain
			}
		}
	}
	return nil
}

func interpretOptions(l *linker, res *parseResult, element descriptorish, opts proto.Message, uninterpreted []*dpb.UninterpretedOption) ([]*dpb.UninterpretedOption, error) {
	optsd, err := loadMessageDescriptorForOptions(l, element.GetFile(), opts)
	if err != nil {
		if res.lenient {
			return uninterpreted, nil
		}
		return nil, res.errs.handleError(err)
	}
	dm := dynamic.NewMessage(optsd)
	err = dm.ConvertFrom(opts)
	if err != nil {
		if res.lenient {
			return uninterpreted, nil
		}
		node := res.nodes[element.AsProto()]
		return nil, res.errs.handleError(ErrorWithSourcePos{Pos: node.Start(), Underlying: err})
	}

	mc := &messageContext{res: res, file: element.GetFile(), elementName: element.GetFullyQualifiedName(), elementType: descriptorType(element.AsProto())}
	var remain []*dpb.UninterpretedOption
	for _, uo := range uninterpreted {
		node := res.getOptionNode(uo)
		if !uo.Name[0].GetIsExtension() && uo.Name[0].GetNamePart() == "uninterpreted_option" {
			if res.lenient {
				remain = append(remain, uo)
				continue
			}
			// uninterpreted_option might be found reflectively, but is not actually valid for use
			if err := res.errs.handleErrorWithPos(node.GetName().Start(), "%vinvalid option 'uninterpreted_option'", mc); err != nil {
				return nil, err
			}
		}
		mc.option = uo
		path, err := interpretField(res, mc, element, dm, uo, 0, nil)
		if err != nil {
			if res.lenient {
				remain = append(remain, uo)
				continue
			}
			return nil, err
		}
		if optn, ok := node.(*ast.OptionNode); ok {
			res.interpretedOptions[optn] = path
		}
	}

	if res.lenient {
		// If we're lenient, then we don't want to clobber the passed in message
		// and leave it partially populated. So we convert into a copy first
		optsClone := proto.Clone(opts)
		if err := dm.ConvertToDeterministic(optsClone); err != nil {
			// TODO: do this in a more granular way, so we can convert individual
			// fields and leave bad ones uninterpreted instead of skipping all of
			// the work we've done so far.
			return uninterpreted, nil
		}
		// conversion from dynamic message above worked, so now
		// it is safe to overwrite the passed in message
		opts.Reset()
		proto.Merge(opts, optsClone)

		return remain, nil
	}

	if err := dm.ValidateRecursive(); err != nil {
		node := res.nodes[element.AsProto()]
		if err := res.errs.handleErrorWithPos(node.Start(), "error in %s options: %v", descriptorType(element.AsProto()), err); err != nil {
			return nil, err
		}
	}

	// now try to convert into the passed in message and fail if not successful
	if err := dm.ConvertToDeterministic(opts); err != nil {
		node := res.nodes[element.AsProto()]
		return nil, res.errs.handleError(ErrorWithSourcePos{Pos: node.Start(), Underlying: err})
	}

	return nil, nil
}

func loadMessageDescriptorForOptions(l *linker, fd fileDescriptorish, opts proto.Message) (*desc.MessageDescriptor, error) {
	// see if the file imports a custom version of descriptor.proto
	fqn := proto.MessageName(opts)
	d := findMessageDescriptorForOptions(l, fd, fqn)
	if d != nil {
		return d, nil
	}
	// fall back to built-in options descriptors
	return desc.LoadMessageDescriptorForMessage(opts)
}

func findMessageDescriptorForOptions(l *linker, fd fileDescriptorish, messageName string) *desc.MessageDescriptor {
	d := fd.FindSymbol(messageName)
	if d != nil {
		md, _ := d.(*desc.MessageDescriptor)
		return md
	}

	// TODO: should this support public imports and be recursive?
	for _, dep := range fd.GetDependencies() {
		d := dep.FindSymbol(messageName)
		if d != nil {
			if l != nil {
				l.markUsed(fd.AsProto().(*dpb.FileDescriptorProto), d.GetFile().AsFileDescriptorProto())
			}
			md, _ := d.(*desc.MessageDescriptor)
			return md
		}
	}

	return nil
}

func interpretField(res *parseResult, mc *messageContext, element descriptorish, dm *dynamic.Message, opt *dpb.UninterpretedOption, nameIndex int, pathPrefix []int32) (path []int32, err error) {
	var fld *desc.FieldDescriptor
	nm := opt.GetName()[nameIndex]
	node := res.getOptionNamePartNode(nm)
	if nm.GetIsExtension() {
		extName := nm.GetNamePart()
		if extName[0] == '.' {
			extName = extName[1:] /* skip leading dot */
		}
		fld = findExtension(element.GetFile(), extName, false, map[fileDescriptorish]struct{}{})
		if fld == nil {
			return nil, res.errs.handleErrorWithPos(node.Start(),
				"%vunrecognized extension %s of %s",
				mc, extName, dm.GetMessageDescriptor().GetFullyQualifiedName())
		}
		if fld.GetOwner().GetFullyQualifiedName() != dm.GetMessageDescriptor().GetFullyQualifiedName() {
			return nil, res.errs.handleErrorWithPos(node.Start(),
				"%vextension %s should extend %s but instead extends %s",
				mc, extName, dm.GetMessageDescriptor().GetFullyQualifiedName(), fld.GetOwner().GetFullyQualifiedName())
		}
	} else {
		fld = dm.GetMessageDescriptor().FindFieldByName(nm.GetNamePart())
		if fld == nil {
			return nil, res.errs.handleErrorWithPos(node.Start(),
				"%vfield %s of %s does not exist",
				mc, nm.GetNamePart(), dm.GetMessageDescriptor().GetFullyQualifiedName())
		}
	}

	path = append(pathPrefix, fld.GetNumber())

	if len(opt.GetName()) > nameIndex+1 {
		nextnm := opt.GetName()[nameIndex+1]
		nextnode := res.getOptionNamePartNode(nextnm)
		if fld.GetType() != dpb.FieldDescriptorProto_TYPE_MESSAGE {
			return nil, res.errs.handleErrorWithPos(nextnode.Start(),
				"%vcannot set field %s because %s is not a message",
				mc, nextnm.GetNamePart(), nm.GetNamePart())
		}
		if fld.IsRepeated() {
			return nil, res.errs.handleErrorWithPos(nextnode.Start(),
				"%vcannot set field %s because %s is repeated (must use an aggregate)",
				mc, nextnm.GetNamePart(), nm.GetNamePart())
		}
		var fdm *dynamic.Message
		var err error
		if dm.HasField(fld) {
			var v interface{}
			v, err = dm.TryGetField(fld)
			fdm, _ = v.(*dynamic.Message)
		} else {
			fdm = dynamic.NewMessage(fld.GetMessageType())
			err = dm.TrySetField(fld, fdm)
		}
		if err != nil {
			return nil, res.errs.handleError(ErrorWithSourcePos{Pos: node.Start(), Underlying: err})
		}
		// recurse to set next part of name
		return interpretField(res, mc, element, fdm, opt, nameIndex+1, path)
	}

	optNode := res.getOptionNode(opt)
	if err := setOptionField(res, mc, dm, fld, node, optNode.GetValue()); err != nil {
		return nil, res.errs.handleError(err)
	}
	if fld.IsRepeated() {
		path = append(path, int32(dm.FieldLength(fld))-1)
	}
	return path, nil
}

func findExtension(fd fileDescriptorish, name string, public bool, checked map[fileDescriptorish]struct{}) *desc.FieldDescriptor {
	if _, ok := checked[fd]; ok {
		return nil
	}
	checked[fd] = struct{}{}
	d := fd.FindSymbol(name)
	if d != nil {
		if fld, ok := d.(*desc.FieldDescriptor); ok {
			return fld
		}
		return nil
	}

	// When public = false, we are searching only directly imported symbols. But we
	// also need to search transitive public imports due to semantics of public imports.
	if public {
		for _, dep := range fd.GetPublicDependencies() {
			d := findExtension(dep, name, true, checked)
			if d != nil {
				return d
			}
		}
	} else {
		for _, dep := range fd.GetDependencies() {
			d := findExtension(dep, name, true, checked)
			if d != nil {
				return d
			}
		}
	}
	return nil
}

func setOptionField(res *parseResult, mc *messageContext, dm *dynamic.Message, fld *desc.FieldDescriptor, name ast.Node, val ast.ValueNode) error {
	v := val.Value()
	if sl, ok := v.([]ast.ValueNode); ok {
		// handle slices a little differently than the others
		if !fld.IsRepeated() {
			return errorWithPos(val.Start(), "%vvalue is an array but field is not repeated", mc)
		}
		origPath := mc.optAggPath
		defer func() {
			mc.optAggPath = origPath
		}()
		for index, item := range sl {
			mc.optAggPath = fmt.Sprintf("%s[%d]", origPath, index)
			if v, err := fieldValue(res, mc, richFldDescriptorish{FieldDescriptor: fld}, item, false); err != nil {
				return err
			} else if err = dm.TryAddRepeatedField(fld, v); err != nil {
				return errorWithPos(val.Start(), "%verror setting value: %s", mc, err)
			}
		}
		return nil
	}

	v, err := fieldValue(res, mc, richFldDescriptorish{FieldDescriptor: fld}, val, false)
	if err != nil {
		return err
	}
	if fld.IsRepeated() {
		err = dm.TryAddRepeatedField(fld, v)
	} else {
		if dm.HasField(fld) {
			return errorWithPos(name.Start(), "%vnon-repeated option field %s already set", mc, fieldName(fld))
		}
		err = dm.TrySetField(fld, v)
	}
	if err != nil {
		return errorWithPos(val.Start(), "%verror setting value: %s", mc, err)
	}

	return nil
}

func findOption(res *parseResult, scope string, opts []*dpb.UninterpretedOption, name string) (int, error) {
	found := -1
	for i, opt := range opts {
		if len(opt.Name) != 1 {
			continue
		}
		if opt.Name[0].GetIsExtension() || opt.Name[0].GetNamePart() != name {
			continue
		}
		if found >= 0 {
			optNode := res.getOptionNode(opt)
			return -1, res.errs.handleErrorWithPos(optNode.GetName().Start(), "%s: option %s cannot be defined more than once", scope, name)
		}
		found = i
	}
	return found, nil
}

func removeOption(uo []*dpb.UninterpretedOption, indexToRemove int) []*dpb.UninterpretedOption {
	if indexToRemove == 0 {
		return uo[1:]
	} else if int(indexToRemove) == len(uo)-1 {
		return uo[:len(uo)-1]
	} else {
		return append(uo[:indexToRemove], uo[indexToRemove+1:]...)
	}
}

type messageContext struct {
	res         *parseResult
	file        fileDescriptorish
	elementType string
	elementName string
	option      *dpb.UninterpretedOption
	optAggPath  string
}

func (c *messageContext) String() string {
	var ctx bytes.Buffer
	if c.elementType != "file" {
		_, _ = fmt.Fprintf(&ctx, "%s %s: ", c.elementType, c.elementName)
	}
	if c.option != nil && c.option.Name != nil {
		ctx.WriteString("option ")
		writeOptionName(&ctx, c.option.Name)
		if c.res.nodes == nil {
			// if we have no source position info, try to provide as much context
			// as possible (if nodes != nil, we don't need this because any errors
			// will actually have file and line numbers)
			if c.optAggPath != "" {
				_, _ = fmt.Fprintf(&ctx, " at %s", c.optAggPath)
			}
		}
		ctx.WriteString(": ")
	}
	return ctx.String()
}

func writeOptionName(buf *bytes.Buffer, parts []*dpb.UninterpretedOption_NamePart) {
	first := true
	for _, p := range parts {
		if first {
			first = false
		} else {
			buf.WriteByte('.')
		}
		nm := p.GetNamePart()
		if nm[0] == '.' {
			// skip leading dot
			nm = nm[1:]
		}
		if p.GetIsExtension() {
			buf.WriteByte('(')
			buf.WriteString(nm)
			buf.WriteByte(')')
		} else {
			buf.WriteString(nm)
		}
	}
}

func fieldName(fld *desc.FieldDescriptor) string {
	if fld.IsExtension() {
		return fld.GetFullyQualifiedName()
	} else {
		return fld.GetName()
	}
}

func valueKind(val interface{}) string {
	switch val := val.(type) {
	case ast.Identifier:
		return "identifier"
	case bool:
		return "bool"
	case int64:
		if val < 0 {
			return "negative integer"
		}
		return "integer"
	case uint64:
		return "integer"
	case float64:
		return "double"
	case string, []byte:
		return "string"
	case []*ast.MessageFieldNode:
		return "message"
	case []ast.ValueNode:
		return "array"
	default:
		return fmt.Sprintf("%T", val)
	}
}

func fieldValue(res *parseResult, mc *messageContext, fld fldDescriptorish, val ast.ValueNode, enumAsString bool) (interface{}, error) {
	v := val.Value()
	t := fld.AsFieldDescriptorProto().GetType()
	switch t {
	case dpb.FieldDescriptorProto_TYPE_ENUM:
		if id, ok := v.(ast.Identifier); ok {
			ev := fld.GetEnumType().FindValueByName(string(id))
			if ev == nil {
				return nil, errorWithPos(val.Start(), "%venum %s has no value named %s", mc, fld.GetEnumType().GetFullyQualifiedName(), id)
			}
			if enumAsString {
				return ev.GetName(), nil
			} else {
				return ev.GetNumber(), nil
			}
		}
		return nil, errorWithPos(val.Start(), "%vexpecting enum, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_MESSAGE, dpb.FieldDescriptorProto_TYPE_GROUP:
		if aggs, ok := v.([]*ast.MessageFieldNode); ok {
			fmd := fld.GetMessageType()
			fdm := dynamic.NewMessage(fmd)
			origPath := mc.optAggPath
			defer func() {
				mc.optAggPath = origPath
			}()
			for _, a := range aggs {
				if origPath == "" {
					mc.optAggPath = a.Name.Value()
				} else {
					mc.optAggPath = origPath + "." + a.Name.Value()
				}
				var ffld *desc.FieldDescriptor
				if a.Name.IsExtension() {
					n := string(a.Name.Name.AsIdentifier())
					ffld = findExtension(mc.file, n, false, map[fileDescriptorish]struct{}{})
					if ffld == nil {
						// may need to qualify with package name
						pkg := mc.file.GetPackage()
						if pkg != "" {
							ffld = findExtension(mc.file, pkg+"."+n, false, map[fileDescriptorish]struct{}{})
						}
					}
				} else {
					ffld = fmd.FindFieldByName(a.Name.Value())
				}
				if ffld == nil {
					return nil, errorWithPos(val.Start(), "%vfield %s not found", mc, string(a.Name.Name.AsIdentifier()))
				}
				if err := setOptionField(res, mc, fdm, ffld, a.Name, a.Val); err != nil {
					return nil, err
				}
			}
			return fdm, nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting message, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting bool, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		if str, ok := v.(string); ok {
			return []byte(str), nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting bytes, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_STRING:
		if str, ok := v.(string); ok {
			return str, nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting string, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SINT32, dpb.FieldDescriptorProto_TYPE_SFIXED32:
		if i, ok := v.(int64); ok {
			if i > math.MaxInt32 || i < math.MinInt32 {
				return nil, errorWithPos(val.Start(), "%vvalue %d is out of range for int32", mc, i)
			}
			return int32(i), nil
		}
		if ui, ok := v.(uint64); ok {
			if ui > math.MaxInt32 {
				return nil, errorWithPos(val.Start(), "%vvalue %d is out of range for int32", mc, ui)
			}
			return int32(ui), nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting int32, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_UINT32, dpb.FieldDescriptorProto_TYPE_FIXED32:
		if i, ok := v.(int64); ok {
			if i > math.MaxUint32 || i < 0 {
				return nil, errorWithPos(val.Start(), "%vvalue %d is out of range for uint32", mc, i)
			}
			return uint32(i), nil
		}
		if ui, ok := v.(uint64); ok {
			if ui > math.MaxUint32 {
				return nil, errorWithPos(val.Start(), "%vvalue %d is out of range for uint32", mc, ui)
			}
			return uint32(ui), nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting uint32, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SINT64, dpb.FieldDescriptorProto_TYPE_SFIXED64:
		if i, ok := v.(int64); ok {
			return i, nil
		}
		if ui, ok := v.(uint64); ok {
			if ui > math.MaxInt64 {
				return nil, errorWithPos(val.Start(), "%vvalue %d is out of range for int64", mc, ui)
			}
			return int64(ui), nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting int64, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_UINT64, dpb.FieldDescriptorProto_TYPE_FIXED64:
		if i, ok := v.(int64); ok {
			if i < 0 {
				return nil, errorWithPos(val.Start(), "%vvalue %d is out of range for uint64", mc, i)
			}
			return uint64(i), nil
		}
		if ui, ok := v.(uint64); ok {
			return ui, nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting uint64, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		if d, ok := v.(float64); ok {
			return d, nil
		}
		if i, ok := v.(int64); ok {
			return float64(i), nil
		}
		if u, ok := v.(uint64); ok {
			return float64(u), nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting double, got %s", mc, valueKind(v))
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		if d, ok := v.(float64); ok {
			if (d > math.MaxFloat32 || d < -math.MaxFloat32) && !math.IsInf(d, 1) && !math.IsInf(d, -1) && !math.IsNaN(d) {
				return nil, errorWithPos(val.Start(), "%vvalue %f is out of range for float", mc, d)
			}
			return float32(d), nil
		}
		if i, ok := v.(int64); ok {
			return float32(i), nil
		}
		if u, ok := v.(uint64); ok {
			return float32(u), nil
		}
		return nil, errorWithPos(val.Start(), "%vexpecting float, got %s", mc, valueKind(v))
	default:
		return nil, errorWithPos(val.Start(), "%vunrecognized field type: %s", mc, t)
	}
}
