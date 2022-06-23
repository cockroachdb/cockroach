package protoparse

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/internal"
	"github.com/jhump/protoreflect/desc/protoparse/ast"
)

type linker struct {
	files             map[string]*parseResult
	filenames         []string
	errs              *errorHandler
	descriptorPool    map[*dpb.FileDescriptorProto]map[string]proto.Message
	packageNamespaces map[*dpb.FileDescriptorProto]map[string]struct{}
	extensions        map[string]map[int32]string
	usedImports       map[*dpb.FileDescriptorProto]map[string]struct{}
}

func newLinker(files *parseResults, errs *errorHandler) *linker {
	return &linker{files: files.resultsByFilename, filenames: files.filenames, errs: errs}
}

func (l *linker) linkFiles() (map[string]*desc.FileDescriptor, error) {
	// First, we put all symbols into a single pool, which lets us ensure there
	// are no duplicate symbols and will also let us resolve and revise all type
	// references in next step.
	if err := l.createDescriptorPool(); err != nil {
		return nil, err
	}

	// After we've populated the pool, we can now try to resolve all type
	// references. All references must be checked for correct type, any fields
	// with enum types must be corrected (since we parse them as if they are
	// message references since we don't actually know message or enum until
	// link time), and references will be re-written to be fully-qualified
	// references (e.g. start with a dot ".").
	if err := l.resolveReferences(); err != nil {
		return nil, err
	}

	if err := l.errs.getError(); err != nil {
		// we won't be able to create real descriptors if we've encountered
		// errors up to this point, so bail at this point
		return nil, err
	}

	// Now we've validated the descriptors, so we can link them into rich
	// descriptors. This is a little redundant since that step does similar
	// checking of symbols. But, without breaking encapsulation (e.g. exporting
	// a lot of fields from desc package that are currently unexported) or
	// merging this into the same package, we can't really prevent it.
	linked, err := l.createdLinkedDescriptors()
	if err != nil {
		return nil, err
	}

	// Now that we have linked descriptors, we can interpret any uninterpreted
	// options that remain.
	for _, r := range l.files {
		fd := linked[r.fd.GetName()]
		if err := interpretFileOptions(l, r, richFileDescriptorish{FileDescriptor: fd}); err != nil {
			return nil, err
		}
		// we should now have any message_set_wire_format options parsed
		// and can do further validation on tag ranges
		if err := checkExtensionsInFile(fd, r); err != nil {
			return nil, err
		}
	}

	// When Parser calls linkFiles, it does not check errs again, and it expects that linkFiles
	// will return all errors it should process. If the ErrorReporter handles all errors itself
	// and always returns nil, we should get ErrInvalidSource here, and need to propagate this
	if err := l.errs.getError(); err != nil {
		return nil, err
	}
	return linked, nil
}

func (l *linker) createDescriptorPool() error {
	l.descriptorPool = map[*dpb.FileDescriptorProto]map[string]proto.Message{}
	l.packageNamespaces = map[*dpb.FileDescriptorProto]map[string]struct{}{}
	for _, filename := range l.filenames {
		r := l.files[filename]
		fd := r.fd
		pool := map[string]proto.Message{}
		l.descriptorPool[fd] = pool
		prefix := fd.GetPackage()
		l.packageNamespaces[fd] = namespacesFromPackage(prefix)
		if prefix != "" {
			prefix += "."
		}
		for _, md := range fd.MessageType {
			if err := addMessageToPool(r, pool, l.errs, prefix, md); err != nil {
				return err
			}
		}
		for _, fld := range fd.Extension {
			if err := addFieldToPool(r, pool, l.errs, prefix, fld); err != nil {
				return err
			}
		}
		for _, ed := range fd.EnumType {
			if err := addEnumToPool(r, pool, l.errs, prefix, ed); err != nil {
				return err
			}
		}
		for _, sd := range fd.Service {
			if err := addServiceToPool(r, pool, l.errs, prefix, sd); err != nil {
				return err
			}
		}
	}
	// try putting everything into a single pool, to ensure there are no duplicates
	// across files (e.g. same symbol, but declared in two different files)
	type entry struct {
		file string
		msg  proto.Message
	}
	pool := map[string]entry{}
	for _, filename := range l.filenames {
		f := l.files[filename].fd
		p := l.descriptorPool[f]
		keys := make([]string, 0, len(p))
		for k := range p {
			keys = append(keys, k)
		}
		sort.Strings(keys) // for deterministic error reporting
		for _, k := range keys {
			v := p[k]
			if e, ok := pool[k]; ok {
				desc1 := e.msg
				file1 := e.file
				desc2 := v
				file2 := f.GetName()
				if file2 < file1 {
					file1, file2 = file2, file1
					desc1, desc2 = desc2, desc1
				}
				node := l.files[file2].nodes[desc2]
				if err := l.errs.handleErrorWithPos(node.Start(), "duplicate symbol %s: already defined as %s in %q", k, descriptorType(desc1), file1); err != nil {
					return err
				}
			}
			pool[k] = entry{file: f.GetName(), msg: v}
		}
	}

	return nil
}

func namespacesFromPackage(pkg string) map[string]struct{} {
	if pkg == "" {
		return nil
	}
	offs := 0
	pkgs := map[string]struct{}{}
	pkgs[pkg] = struct{}{}
	for {
		pos := strings.IndexByte(pkg[offs:], '.')
		if pos == -1 {
			return pkgs
		}
		pkgs[pkg[:offs+pos]] = struct{}{}
		offs = offs + pos + 1
	}
}

func addMessageToPool(r *parseResult, pool map[string]proto.Message, errs *errorHandler, prefix string, md *dpb.DescriptorProto) error {
	fqn := prefix + md.GetName()
	if err := addToPool(r, pool, errs, fqn, md); err != nil {
		return err
	}
	prefix = fqn + "."
	for _, fld := range md.Field {
		if err := addFieldToPool(r, pool, errs, prefix, fld); err != nil {
			return err
		}
	}
	for _, fld := range md.Extension {
		if err := addFieldToPool(r, pool, errs, prefix, fld); err != nil {
			return err
		}
	}
	for _, nmd := range md.NestedType {
		if err := addMessageToPool(r, pool, errs, prefix, nmd); err != nil {
			return err
		}
	}
	for _, ed := range md.EnumType {
		if err := addEnumToPool(r, pool, errs, prefix, ed); err != nil {
			return err
		}
	}
	return nil
}

func addFieldToPool(r *parseResult, pool map[string]proto.Message, errs *errorHandler, prefix string, fld *dpb.FieldDescriptorProto) error {
	fqn := prefix + fld.GetName()
	return addToPool(r, pool, errs, fqn, fld)
}

func addEnumToPool(r *parseResult, pool map[string]proto.Message, errs *errorHandler, prefix string, ed *dpb.EnumDescriptorProto) error {
	fqn := prefix + ed.GetName()
	if err := addToPool(r, pool, errs, fqn, ed); err != nil {
		return err
	}
	for _, evd := range ed.Value {
		// protobuf name-scoping rules for enum values follow C++ scoping rules:
		// the enum value name is a symbol in the *parent* scope (the one
		// enclosing the enum).
		vfqn := prefix + evd.GetName()
		if err := addToPool(r, pool, errs, vfqn, evd); err != nil {
			return err
		}
	}
	return nil
}

func addServiceToPool(r *parseResult, pool map[string]proto.Message, errs *errorHandler, prefix string, sd *dpb.ServiceDescriptorProto) error {
	fqn := prefix + sd.GetName()
	if err := addToPool(r, pool, errs, fqn, sd); err != nil {
		return err
	}
	for _, mtd := range sd.Method {
		mfqn := fqn + "." + mtd.GetName()
		if err := addToPool(r, pool, errs, mfqn, mtd); err != nil {
			return err
		}
	}
	return nil
}

func addToPool(r *parseResult, pool map[string]proto.Message, errs *errorHandler, fqn string, dsc proto.Message) error {
	if d, ok := pool[fqn]; ok {
		node := r.nodes[dsc]
		_, additionIsEnumVal := dsc.(*dpb.EnumValueDescriptorProto)
		_, existingIsEnumVal := d.(*dpb.EnumValueDescriptorProto)
		// because of weird scoping for enum values, provide more context in error message
		// if this conflict is with an enum value
		var suffix string
		if additionIsEnumVal || existingIsEnumVal {
			suffix = "; protobuf uses C++ scoping rules for enum values, so they exist in the scope enclosing the enum"
		}
		// TODO: also include the source location for the conflicting symbol
		if err := errs.handleErrorWithPos(node.Start(), "duplicate symbol %s: already defined as %s%s", fqn, descriptorType(d), suffix); err != nil {
			return err
		}
	}
	pool[fqn] = dsc
	return nil
}

func descriptorType(m proto.Message) string {
	switch m := m.(type) {
	case *dpb.DescriptorProto:
		return "message"
	case *dpb.DescriptorProto_ExtensionRange:
		return "extension range"
	case *dpb.FieldDescriptorProto:
		if m.GetExtendee() == "" {
			return "field"
		} else {
			return "extension"
		}
	case *dpb.EnumDescriptorProto:
		return "enum"
	case *dpb.EnumValueDescriptorProto:
		return "enum value"
	case *dpb.ServiceDescriptorProto:
		return "service"
	case *dpb.MethodDescriptorProto:
		return "method"
	case *dpb.FileDescriptorProto:
		return "file"
	default:
		// shouldn't be possible
		return fmt.Sprintf("%T", m)
	}
}

func (l *linker) resolveReferences() error {
	l.extensions = map[string]map[int32]string{}
	l.usedImports = map[*dpb.FileDescriptorProto]map[string]struct{}{}
	for _, filename := range l.filenames {
		r := l.files[filename]
		fd := r.fd
		prefix := fd.GetPackage()
		scopes := []scope{fileScope(fd, l)}
		if prefix != "" {
			prefix += "."
		}
		if fd.Options != nil {
			if err := l.resolveOptions(r, fd, "file", fd.GetName(), proto.MessageName(fd.Options), fd.Options.UninterpretedOption, scopes); err != nil {
				return err
			}
		}
		for _, md := range fd.MessageType {
			if err := l.resolveMessageTypes(r, fd, prefix, md, scopes); err != nil {
				return err
			}
		}
		for _, fld := range fd.Extension {
			if err := l.resolveFieldTypes(r, fd, prefix, fld, scopes); err != nil {
				return err
			}
		}
		for _, ed := range fd.EnumType {
			if err := l.resolveEnumTypes(r, fd, prefix, ed, scopes); err != nil {
				return err
			}
		}
		for _, sd := range fd.Service {
			if err := l.resolveServiceTypes(r, fd, prefix, sd, scopes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *linker) resolveEnumTypes(r *parseResult, fd *dpb.FileDescriptorProto, prefix string, ed *dpb.EnumDescriptorProto, scopes []scope) error {
	enumFqn := prefix + ed.GetName()
	if ed.Options != nil {
		if err := l.resolveOptions(r, fd, "enum", enumFqn, proto.MessageName(ed.Options), ed.Options.UninterpretedOption, scopes); err != nil {
			return err
		}
	}
	for _, evd := range ed.Value {
		if evd.Options != nil {
			evFqn := enumFqn + "." + evd.GetName()
			if err := l.resolveOptions(r, fd, "enum value", evFqn, proto.MessageName(evd.Options), evd.Options.UninterpretedOption, scopes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *linker) resolveMessageTypes(r *parseResult, fd *dpb.FileDescriptorProto, prefix string, md *dpb.DescriptorProto, scopes []scope) error {
	fqn := prefix + md.GetName()
	scope := messageScope(fqn, isProto3(fd), l, fd)
	scopes = append(scopes, scope)
	prefix = fqn + "."

	if md.Options != nil {
		if err := l.resolveOptions(r, fd, "message", fqn, proto.MessageName(md.Options), md.Options.UninterpretedOption, scopes); err != nil {
			return err
		}
	}

	for _, nmd := range md.NestedType {
		if err := l.resolveMessageTypes(r, fd, prefix, nmd, scopes); err != nil {
			return err
		}
	}
	for _, ned := range md.EnumType {
		if err := l.resolveEnumTypes(r, fd, prefix, ned, scopes); err != nil {
			return err
		}
	}
	for _, fld := range md.Field {
		if err := l.resolveFieldTypes(r, fd, prefix, fld, scopes); err != nil {
			return err
		}
	}
	for _, ood := range md.OneofDecl {
		if ood.Options != nil {
			ooName := fmt.Sprintf("%s.%s", fqn, ood.GetName())
			if err := l.resolveOptions(r, fd, "oneof", ooName, proto.MessageName(ood.Options), ood.Options.UninterpretedOption, scopes); err != nil {
				return err
			}
		}
	}
	for _, fld := range md.Extension {
		if err := l.resolveFieldTypes(r, fd, prefix, fld, scopes); err != nil {
			return err
		}
	}
	for _, er := range md.ExtensionRange {
		if er.Options != nil {
			erName := fmt.Sprintf("%s:%d-%d", fqn, er.GetStart(), er.GetEnd()-1)
			if err := l.resolveOptions(r, fd, "extension range", erName, proto.MessageName(er.Options), er.Options.UninterpretedOption, scopes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *linker) resolveFieldTypes(r *parseResult, fd *dpb.FileDescriptorProto, prefix string, fld *dpb.FieldDescriptorProto, scopes []scope) error {
	thisName := prefix + fld.GetName()
	scope := fmt.Sprintf("field %s", thisName)
	node := r.getFieldNode(fld)
	elemType := "field"
	if fld.GetExtendee() != "" {
		elemType = "extension"
		fqn, dsc, _ := l.resolve(fd, fld.GetExtendee(), true, scopes)
		if dsc == nil {
			return l.errs.handleErrorWithPos(node.FieldExtendee().Start(), "unknown extendee type %s", fld.GetExtendee())
		}
		if dsc == sentinelMissingSymbol {
			return l.errs.handleErrorWithPos(node.FieldExtendee().Start(), "unknown extendee type %s; resolved to %s which is not defined; consider using a leading dot", fld.GetExtendee(), fqn)
		}
		extd, ok := dsc.(*dpb.DescriptorProto)
		if !ok {
			otherType := descriptorType(dsc)
			return l.errs.handleErrorWithPos(node.FieldExtendee().Start(), "extendee is invalid: %s is a %s, not a message", fqn, otherType)
		}
		fld.Extendee = proto.String("." + fqn)
		// make sure the tag number is in range
		found := false
		tag := fld.GetNumber()
		for _, rng := range extd.ExtensionRange {
			if tag >= rng.GetStart() && tag < rng.GetEnd() {
				found = true
				break
			}
		}
		if !found {
			if err := l.errs.handleErrorWithPos(node.FieldTag().Start(), "%s: tag %d is not in valid range for extended type %s", scope, tag, fqn); err != nil {
				return err
			}
		} else {
			// make sure tag is not a duplicate
			usedExtTags := l.extensions[fqn]
			if usedExtTags == nil {
				usedExtTags = map[int32]string{}
				l.extensions[fqn] = usedExtTags
			}
			if other := usedExtTags[fld.GetNumber()]; other != "" {
				if err := l.errs.handleErrorWithPos(node.FieldTag().Start(), "%s: duplicate extension: %s and %s are both using tag %d", scope, other, thisName, fld.GetNumber()); err != nil {
					return err
				}
			} else {
				usedExtTags[fld.GetNumber()] = thisName
			}
		}
	}

	if fld.Options != nil {
		if err := l.resolveOptions(r, fd, elemType, thisName, proto.MessageName(fld.Options), fld.Options.UninterpretedOption, scopes); err != nil {
			return err
		}
	}

	if fld.GetTypeName() == "" {
		// scalar type; no further resolution required
		return nil
	}

	fqn, dsc, proto3 := l.resolve(fd, fld.GetTypeName(), true, scopes)
	if dsc == nil {
		return l.errs.handleErrorWithPos(node.FieldType().Start(), "%s: unknown type %s", scope, fld.GetTypeName())
	}
	if dsc == sentinelMissingSymbol {
		return l.errs.handleErrorWithPos(node.FieldType().Start(), "%s: unknown type %s; resolved to %s which is not defined; consider using a leading dot", scope, fld.GetTypeName(), fqn)
	}
	switch dsc := dsc.(type) {
	case *dpb.DescriptorProto:
		fld.TypeName = proto.String("." + fqn)
		// if type was tentatively unset, we now know it's actually a message
		if fld.Type == nil {
			fld.Type = dpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		}
	case *dpb.EnumDescriptorProto:
		if fld.GetExtendee() == "" && isProto3(fd) && !proto3 {
			// fields in a proto3 message cannot refer to proto2 enums
			return l.errs.handleErrorWithPos(node.FieldType().Start(), "%s: cannot use proto2 enum %s in a proto3 message", scope, fld.GetTypeName())
		}
		fld.TypeName = proto.String("." + fqn)
		// the type was tentatively unset, but now we know it's actually an enum
		fld.Type = dpb.FieldDescriptorProto_TYPE_ENUM.Enum()
	default:
		otherType := descriptorType(dsc)
		return l.errs.handleErrorWithPos(node.FieldType().Start(), "%s: invalid type: %s is a %s, not a message or enum", scope, fqn, otherType)
	}
	return nil
}

func (l *linker) resolveServiceTypes(r *parseResult, fd *dpb.FileDescriptorProto, prefix string, sd *dpb.ServiceDescriptorProto, scopes []scope) error {
	svcFqn := prefix + sd.GetName()
	if sd.Options != nil {
		if err := l.resolveOptions(r, fd, "service", svcFqn, proto.MessageName(sd.Options), sd.Options.UninterpretedOption, scopes); err != nil {
			return err
		}
	}

	// not a message, but same scoping rules for nested elements as if it were
	scope := messageScope(svcFqn, isProto3(fd), l, fd)
	scopes = append(scopes, scope)

	for _, mtd := range sd.Method {
		if mtd.Options != nil {
			if err := l.resolveOptions(r, fd, "method", svcFqn+"."+mtd.GetName(), proto.MessageName(mtd.Options), mtd.Options.UninterpretedOption, scopes); err != nil {
				return err
			}
		}
		scope := fmt.Sprintf("method %s.%s", svcFqn, mtd.GetName())
		node := r.getMethodNode(mtd)
		fqn, dsc, _ := l.resolve(fd, mtd.GetInputType(), false, scopes)
		if dsc == nil {
			if err := l.errs.handleErrorWithPos(node.GetInputType().Start(), "%s: unknown request type %s", scope, mtd.GetInputType()); err != nil {
				return err
			}
		} else if dsc == sentinelMissingSymbol {
			if err := l.errs.handleErrorWithPos(node.GetInputType().Start(), "%s: unknown request type %s; resolved to %s which is not defined; consider using a leading dot", scope, mtd.GetInputType(), fqn); err != nil {
				return err
			}
		} else if _, ok := dsc.(*dpb.DescriptorProto); !ok {
			otherType := descriptorType(dsc)
			if err := l.errs.handleErrorWithPos(node.GetInputType().Start(), "%s: invalid request type: %s is a %s, not a message", scope, fqn, otherType); err != nil {
				return err
			}
		} else {
			mtd.InputType = proto.String("." + fqn)
		}

		// TODO: make input and output type resolution more DRY
		fqn, dsc, _ = l.resolve(fd, mtd.GetOutputType(), false, scopes)
		if dsc == nil {
			if err := l.errs.handleErrorWithPos(node.GetOutputType().Start(), "%s: unknown response type %s", scope, mtd.GetOutputType()); err != nil {
				return err
			}
		} else if dsc == sentinelMissingSymbol {
			if err := l.errs.handleErrorWithPos(node.GetOutputType().Start(), "%s: unknown response type %s; resolved to %s which is not defined; consider using a leading dot", scope, mtd.GetOutputType(), fqn); err != nil {
				return err
			}
		} else if _, ok := dsc.(*dpb.DescriptorProto); !ok {
			otherType := descriptorType(dsc)
			if err := l.errs.handleErrorWithPos(node.GetOutputType().Start(), "%s: invalid response type: %s is a %s, not a message", scope, fqn, otherType); err != nil {
				return err
			}
		} else {
			mtd.OutputType = proto.String("." + fqn)
		}
	}
	return nil
}

func (l *linker) resolveOptions(r *parseResult, fd *dpb.FileDescriptorProto, elemType, elemName, optType string, opts []*dpb.UninterpretedOption, scopes []scope) error {
	var scope string
	if elemType != "file" {
		scope = fmt.Sprintf("%s %s: ", elemType, elemName)
	}
opts:
	for _, opt := range opts {
		for _, nm := range opt.Name {
			if nm.GetIsExtension() {
				node := r.getOptionNamePartNode(nm)
				fqn, dsc, _ := l.resolve(fd, nm.GetNamePart(), false, scopes)
				if dsc == nil {
					if err := l.errs.handleErrorWithPos(node.Start(), "%sunknown extension %s", scope, nm.GetNamePart()); err != nil {
						return err
					}
					continue opts
				}
				if dsc == sentinelMissingSymbol {
					if err := l.errs.handleErrorWithPos(node.Start(), "%sunknown extension %s; resolved to %s which is not defined; consider using a leading dot", scope, nm.GetNamePart(), fqn); err != nil {
						return err
					}
					continue opts
				}
				if ext, ok := dsc.(*dpb.FieldDescriptorProto); !ok {
					otherType := descriptorType(dsc)
					if err := l.errs.handleErrorWithPos(node.Start(), "%sinvalid extension: %s is a %s, not an extension", scope, nm.GetNamePart(), otherType); err != nil {
						return err
					}
					continue opts
				} else if ext.GetExtendee() == "" {
					if err := l.errs.handleErrorWithPos(node.Start(), "%sinvalid extension: %s is a field but not an extension", scope, nm.GetNamePart()); err != nil {
						return err
					}
					continue opts
				}
				nm.NamePart = proto.String("." + fqn)
			}
		}
	}
	return nil
}

func (l *linker) resolve(fd *dpb.FileDescriptorProto, name string, onlyTypes bool, scopes []scope) (fqn string, element proto.Message, proto3 bool) {
	if strings.HasPrefix(name, ".") {
		// already fully-qualified
		d, proto3 := l.findSymbol(fd, name[1:])
		if d != nil {
			return name[1:], d, proto3
		}
		return "", nil, false
	}
	// unqualified, so we look in the enclosing (last) scope first and move
	// towards outermost (first) scope, trying to resolve the symbol
	pos := strings.IndexByte(name, '.')
	firstName := name
	if pos > 0 {
		firstName = name[:pos]
	}
	var bestGuess proto.Message
	var bestGuessFqn string
	var bestGuessProto3 bool
	for i := len(scopes) - 1; i >= 0; i-- {
		fqn, d, proto3 := scopes[i](firstName, name)
		if d != nil {
			if !onlyTypes || isType(d) {
				return fqn, d, proto3
			} else if bestGuess == nil {
				bestGuess = d
				bestGuessFqn = fqn
				bestGuessProto3 = proto3
			}
		}
	}
	// we return best guess, even though it was not an allowed kind of
	// descriptor, so caller can print a better error message (e.g.
	// indicating that the name was found but that it's the wrong type)
	return bestGuessFqn, bestGuess, bestGuessProto3
}

func isType(m proto.Message) bool {
	switch m.(type) {
	case *dpb.DescriptorProto, *dpb.EnumDescriptorProto:
		return true
	}
	return false
}

// scope represents a lexical scope in a proto file in which messages and enums
// can be declared.
type scope func(firstName, fullName string) (fqn string, element proto.Message, proto3 bool)

func fileScope(fd *dpb.FileDescriptorProto, l *linker) scope {
	// we search symbols in this file, but also symbols in other files that have
	// the same package as this file or a "parent" package (in protobuf,
	// packages are a hierarchy like C++ namespaces)
	prefixes := internal.CreatePrefixList(fd.GetPackage())
	querySymbol := func(n string) (d proto.Message, isProto3 bool) {
		return l.findSymbol(fd, n)
	}
	return func(firstName, fullName string) (string, proto.Message, bool) {
		for _, prefix := range prefixes {
			var n1, n string
			if prefix == "" {
				// exhausted all prefixes, so it must be in this one
				n1, n = fullName, fullName
			} else {
				n = prefix + "." + fullName
				n1 = prefix + "." + firstName
			}
			d, proto3 := findSymbolRelative(n1, n, querySymbol)
			if d != nil {
				return n, d, proto3
			}
		}
		return "", nil, false
	}
}

func messageScope(messageName string, proto3 bool, l *linker, fd *dpb.FileDescriptorProto) scope {
	querySymbol := func(n string) (d proto.Message, isProto3 bool) {
		return l.findSymbolInFile(n, fd), false
	}
	return func(firstName, fullName string) (string, proto.Message, bool) {
		n1 := messageName + "." + firstName
		n := messageName + "." + fullName
		d, _ := findSymbolRelative(n1, n, querySymbol)
		if d != nil {
			return n, d, proto3
		}
		return "", nil, false
	}
}

func findSymbolRelative(firstName, fullName string, query func(name string) (d proto.Message, isProto3 bool)) (d proto.Message, isProto3 bool) {
	d, proto3 := query(firstName)
	if d == nil {
		return nil, false
	}
	if firstName == fullName {
		return d, proto3
	}
	if !isAggregateDescriptor(d) {
		// can't possibly find the rest of full name if
		// the first name indicated a leaf descriptor
		return nil, false
	}
	d, proto3 = query(fullName)
	if d == nil {
		return sentinelMissingSymbol, false
	}
	return d, proto3
}

func (l *linker) findSymbolInFile(name string, fd *dpb.FileDescriptorProto) proto.Message {
	d, ok := l.descriptorPool[fd][name]
	if ok {
		return d
	}
	_, ok = l.packageNamespaces[fd][name]
	if ok {
		// this sentinel means the name is a valid namespace but
		// does not refer to a descriptor
		return sentinelMissingSymbol
	}
	return nil
}

func (l *linker) markUsed(entryPoint, used *dpb.FileDescriptorProto) {
	importsForFile := l.usedImports[entryPoint]
	if importsForFile == nil {
		importsForFile = map[string]struct{}{}
		l.usedImports[entryPoint] = importsForFile
	}
	importsForFile[used.GetName()] = struct{}{}
}

func isAggregateDescriptor(m proto.Message) bool {
	if m == sentinelMissingSymbol {
		// this indicates the name matched a package, not a
		// descriptor, but a package is an aggregate so
		// we return true
		return true
	}
	switch m.(type) {
	case *dpb.DescriptorProto, *dpb.EnumDescriptorProto, *dpb.ServiceDescriptorProto:
		return true
	default:
		return false
	}
}

// This value is a bogus/nil value, but results in a non-nil
// proto.Message interface value. So we use it as a sentinel
// to indicate "stop searching for symbol... because it
// definitively does not exist".
var sentinelMissingSymbol = (*dpb.DescriptorProto)(nil)

func (l *linker) findSymbol(fd *dpb.FileDescriptorProto, name string) (element proto.Message, proto3 bool) {
	return l.findSymbolRecursive(fd, fd, name, false, map[*dpb.FileDescriptorProto]struct{}{})
}

func (l *linker) findSymbolRecursive(entryPoint, fd *dpb.FileDescriptorProto, name string, public bool, checked map[*dpb.FileDescriptorProto]struct{}) (element proto.Message, proto3 bool) {
	if _, ok := checked[fd]; ok {
		// already checked this one
		return nil, false
	}
	checked[fd] = struct{}{}
	d := l.findSymbolInFile(name, fd)
	if d != nil {
		return d, isProto3(fd)
	}

	// When public = false, we are searching only directly imported symbols. But we
	// also need to search transitive public imports due to semantics of public imports.
	if public {
		for _, depIndex := range fd.PublicDependency {
			dep := fd.Dependency[depIndex]
			depres := l.files[dep]
			if depres == nil {
				// we'll catch this error later
				continue
			}
			if d, proto3 := l.findSymbolRecursive(entryPoint, depres.fd, name, true, checked); d != nil {
				l.markUsed(entryPoint, depres.fd)
				return d, proto3
			}
		}
	} else {
		for _, dep := range fd.Dependency {
			depres := l.files[dep]
			if depres == nil {
				// we'll catch this error later
				continue
			}
			if d, proto3 := l.findSymbolRecursive(entryPoint, depres.fd, name, true, checked); d != nil {
				l.markUsed(entryPoint, depres.fd)
				return d, proto3
			}
		}
	}

	return nil, false
}

func isProto3(fd *dpb.FileDescriptorProto) bool {
	return fd.GetSyntax() == "proto3"
}

func (l *linker) createdLinkedDescriptors() (map[string]*desc.FileDescriptor, error) {
	names := make([]string, 0, len(l.files))
	for name := range l.files {
		names = append(names, name)
	}
	sort.Strings(names)
	linked := map[string]*desc.FileDescriptor{}
	for _, name := range names {
		if _, err := l.linkFile(name, nil, nil, linked); err != nil {
			return nil, err
		}
	}
	return linked, nil
}

func (l *linker) linkFile(name string, rootImportLoc *SourcePos, seen []string, linked map[string]*desc.FileDescriptor) (*desc.FileDescriptor, error) {
	// check for import cycle
	for _, s := range seen {
		if name == s {
			var msg bytes.Buffer
			first := true
			for _, s := range seen {
				if first {
					first = false
				} else {
					msg.WriteString(" -> ")
				}
				_, _ = fmt.Fprintf(&msg, "%q", s)
			}
			_, _ = fmt.Fprintf(&msg, " -> %q", name)
			return nil, ErrorWithSourcePos{
				Underlying: fmt.Errorf("cycle found in imports: %s", msg.String()),
				Pos:        rootImportLoc,
			}
		}
	}
	seen = append(seen, name)

	if lfd, ok := linked[name]; ok {
		// already linked
		return lfd, nil
	}
	r := l.files[name]
	if r == nil {
		importer := seen[len(seen)-2] // len-1 is *this* file, before that is the one that imported it
		return nil, fmt.Errorf("no descriptor found for %q, imported by %q", name, importer)
	}
	var deps []*desc.FileDescriptor
	if rootImportLoc == nil {
		// try to find a source location for this "root" import
		decl := r.getFileNode(r.fd)
		fnode, ok := decl.(*ast.FileNode)
		if ok {
			for _, decl := range fnode.Decls {
				if dep, ok := decl.(*ast.ImportNode); ok {
					ldep, err := l.linkFile(dep.Name.AsString(), dep.Name.Start(), seen, linked)
					if err != nil {
						return nil, err
					}
					deps = append(deps, ldep)
				}
			}
		} else {
			// no AST? just use the descriptor
			for _, dep := range r.fd.Dependency {
				ldep, err := l.linkFile(dep, decl.Start(), seen, linked)
				if err != nil {
					return nil, err
				}
				deps = append(deps, ldep)
			}
		}
	} else {
		// we can just use the descriptor since we don't need source location
		// (we'll just attribute any import cycles found to the "root" import)
		for _, dep := range r.fd.Dependency {
			ldep, err := l.linkFile(dep, rootImportLoc, seen, linked)
			if err != nil {
				return nil, err
			}
			deps = append(deps, ldep)
		}
	}
	lfd, err := desc.CreateFileDescriptor(r.fd, deps...)
	if err != nil {
		return nil, fmt.Errorf("error linking %q: %s", name, err)
	}
	linked[name] = lfd
	return lfd, nil
}

func (l *linker) checkForUnusedImports(filename string) {
	r := l.files[filename]
	usedImports := l.usedImports[r.fd]
	node := r.nodes[r.fd]
	fileNode, _ := node.(*ast.FileNode)
	for i, dep := range r.fd.Dependency {
		if _, ok := usedImports[dep]; !ok {
			isPublic := false
			// it's fine if it's a public import
			for _, j := range r.fd.PublicDependency {
				if i == int(j) {
					isPublic = true
					break
				}
			}
			if isPublic {
				break
			}
			var pos *SourcePos
			if fileNode != nil {
				for _, decl := range fileNode.Decls {
					imp, ok := decl.(*ast.ImportNode)
					if !ok {
						continue
					}
					if imp.Name.AsString() == dep {
						pos = imp.Start()
					}
				}
			}
			if pos == nil {
				pos = ast.UnknownPos(r.fd.GetName())
			}
			r.errs.warn(pos, errUnusedImport(dep))
		}
	}
}
