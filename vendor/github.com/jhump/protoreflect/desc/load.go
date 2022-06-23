package desc

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/internal"
)

var (
	cacheMu       sync.RWMutex
	filesCache    = map[string]*FileDescriptor{}
	messagesCache = map[string]*MessageDescriptor{}
	enumCache     = map[reflect.Type]*EnumDescriptor{}
)

// LoadFileDescriptor creates a file descriptor using the bytes returned by
// proto.FileDescriptor. Descriptors are cached so that they do not need to be
// re-processed if the same file is fetched again later.
func LoadFileDescriptor(file string) (*FileDescriptor, error) {
	return loadFileDescriptor(file, nil)
}

func loadFileDescriptor(file string, r *ImportResolver) (*FileDescriptor, error) {
	f := getFileFromCache(file)
	if f != nil {
		return f, nil
	}
	cacheMu.Lock()
	defer cacheMu.Unlock()
	return loadFileDescriptorLocked(file, r)
}

func loadFileDescriptorLocked(file string, r *ImportResolver) (*FileDescriptor, error) {
	f := filesCache[file]
	if f != nil {
		return f, nil
	}
	fd, err := internal.LoadFileDescriptor(file)
	if err != nil {
		return nil, err
	}

	f, err = toFileDescriptorLocked(fd, r)
	if err != nil {
		return nil, err
	}
	putCacheLocked(file, f)
	return f, nil
}

func toFileDescriptorLocked(fd *dpb.FileDescriptorProto, r *ImportResolver) (*FileDescriptor, error) {
	deps := make([]*FileDescriptor, len(fd.GetDependency()))
	for i, dep := range fd.GetDependency() {
		resolvedDep := r.ResolveImport(fd.GetName(), dep)
		var err error
		deps[i], err = loadFileDescriptorLocked(resolvedDep, r)
		if _, ok := err.(internal.ErrNoSuchFile); ok && resolvedDep != dep {
			// try original path
			deps[i], err = loadFileDescriptorLocked(dep, r)
		}
		if err != nil {
			return nil, err
		}
	}
	return CreateFileDescriptor(fd, deps...)
}

func getFileFromCache(file string) *FileDescriptor {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	return filesCache[file]
}

func putCacheLocked(filename string, fd *FileDescriptor) {
	filesCache[filename] = fd
	putMessageCacheLocked(fd.messages)
}

func putMessageCacheLocked(mds []*MessageDescriptor) {
	for _, md := range mds {
		messagesCache[md.fqn] = md
		putMessageCacheLocked(md.nested)
	}
}

// interface implemented by generated messages, which all have a Descriptor() method in
// addition to the methods of proto.Message
type protoMessage interface {
	proto.Message
	Descriptor() ([]byte, []int)
}

// LoadMessageDescriptor loads descriptor using the encoded descriptor proto returned by
// Message.Descriptor() for the given message type. If the given type is not recognized,
// then a nil descriptor is returned.
func LoadMessageDescriptor(message string) (*MessageDescriptor, error) {
	return loadMessageDescriptor(message, nil)
}

func loadMessageDescriptor(message string, r *ImportResolver) (*MessageDescriptor, error) {
	m := getMessageFromCache(message)
	if m != nil {
		return m, nil
	}

	pt := proto.MessageType(message)
	if pt == nil {
		return nil, nil
	}
	msg, err := messageFromType(pt)
	if err != nil {
		return nil, err
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()
	return loadMessageDescriptorForTypeLocked(message, msg, r)
}

// LoadMessageDescriptorForType loads descriptor using the encoded descriptor proto returned
// by message.Descriptor() for the given message type. If the given type is not recognized,
// then a nil descriptor is returned.
func LoadMessageDescriptorForType(messageType reflect.Type) (*MessageDescriptor, error) {
	return loadMessageDescriptorForType(messageType, nil)
}

func loadMessageDescriptorForType(messageType reflect.Type, r *ImportResolver) (*MessageDescriptor, error) {
	m, err := messageFromType(messageType)
	if err != nil {
		return nil, err
	}
	return loadMessageDescriptorForMessage(m, r)
}

// LoadMessageDescriptorForMessage loads descriptor using the encoded descriptor proto
// returned by message.Descriptor(). If the given type is not recognized, then a nil
// descriptor is returned.
func LoadMessageDescriptorForMessage(message proto.Message) (*MessageDescriptor, error) {
	return loadMessageDescriptorForMessage(message, nil)
}

func loadMessageDescriptorForMessage(message proto.Message, r *ImportResolver) (*MessageDescriptor, error) {
	// efficiently handle dynamic messages
	type descriptorable interface {
		GetMessageDescriptor() *MessageDescriptor
	}
	if d, ok := message.(descriptorable); ok {
		return d.GetMessageDescriptor(), nil
	}

	name := proto.MessageName(message)
	if name == "" {
		return nil, nil
	}
	m := getMessageFromCache(name)
	if m != nil {
		return m, nil
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()
	return loadMessageDescriptorForTypeLocked(name, message.(protoMessage), nil)
}

func messageFromType(mt reflect.Type) (protoMessage, error) {
	if mt.Kind() != reflect.Ptr {
		mt = reflect.PtrTo(mt)
	}
	m, ok := reflect.Zero(mt).Interface().(protoMessage)
	if !ok {
		return nil, fmt.Errorf("failed to create message from type: %v", mt)
	}
	return m, nil
}

func loadMessageDescriptorForTypeLocked(name string, message protoMessage, r *ImportResolver) (*MessageDescriptor, error) {
	m := messagesCache[name]
	if m != nil {
		return m, nil
	}

	fdb, _ := message.Descriptor()
	fd, err := internal.DecodeFileDescriptor(name, fdb)
	if err != nil {
		return nil, err
	}

	f, err := toFileDescriptorLocked(fd, r)
	if err != nil {
		return nil, err
	}
	putCacheLocked(fd.GetName(), f)
	return f.FindSymbol(name).(*MessageDescriptor), nil
}

func getMessageFromCache(message string) *MessageDescriptor {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	return messagesCache[message]
}

// interface implemented by all generated enums
type protoEnum interface {
	EnumDescriptor() ([]byte, []int)
}

// NB: There is no LoadEnumDescriptor that takes a fully-qualified enum name because
// it is not useful since protoc-gen-go does not expose the name anywhere in generated
// code or register it in a way that is it accessible for reflection code. This also
// means we have to cache enum descriptors differently -- we can only cache them as
// they are requested, as opposed to caching all enum types whenever a file descriptor
// is cached. This is because we need to know the generated type of the enums, and we
// don't know that at the time of caching file descriptors.

// LoadEnumDescriptorForType loads descriptor using the encoded descriptor proto returned
// by enum.EnumDescriptor() for the given enum type.
func LoadEnumDescriptorForType(enumType reflect.Type) (*EnumDescriptor, error) {
	return loadEnumDescriptorForType(enumType, nil)
}

func loadEnumDescriptorForType(enumType reflect.Type, r *ImportResolver) (*EnumDescriptor, error) {
	// we cache descriptors using non-pointer type
	if enumType.Kind() == reflect.Ptr {
		enumType = enumType.Elem()
	}
	e := getEnumFromCache(enumType)
	if e != nil {
		return e, nil
	}
	enum, err := enumFromType(enumType)
	if err != nil {
		return nil, err
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()
	return loadEnumDescriptorForTypeLocked(enumType, enum, r)
}

// LoadEnumDescriptorForEnum loads descriptor using the encoded descriptor proto
// returned by enum.EnumDescriptor().
func LoadEnumDescriptorForEnum(enum protoEnum) (*EnumDescriptor, error) {
	return loadEnumDescriptorForEnum(enum, nil)
}

func loadEnumDescriptorForEnum(enum protoEnum, r *ImportResolver) (*EnumDescriptor, error) {
	et := reflect.TypeOf(enum)
	// we cache descriptors using non-pointer type
	if et.Kind() == reflect.Ptr {
		et = et.Elem()
		enum = reflect.Zero(et).Interface().(protoEnum)
	}
	e := getEnumFromCache(et)
	if e != nil {
		return e, nil
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()
	return loadEnumDescriptorForTypeLocked(et, enum, r)
}

func enumFromType(et reflect.Type) (protoEnum, error) {
	if et.Kind() != reflect.Int32 {
		et = reflect.PtrTo(et)
	}
	e, ok := reflect.Zero(et).Interface().(protoEnum)
	if !ok {
		return nil, fmt.Errorf("failed to create enum from type: %v", et)
	}
	return e, nil
}

func loadEnumDescriptorForTypeLocked(et reflect.Type, enum protoEnum, r *ImportResolver) (*EnumDescriptor, error) {
	e := enumCache[et]
	if e != nil {
		return e, nil
	}

	fdb, path := enum.EnumDescriptor()
	name := fmt.Sprintf("%v", et)
	fd, err := internal.DecodeFileDescriptor(name, fdb)
	if err != nil {
		return nil, err
	}
	// see if we already have cached "rich" descriptor
	f, ok := filesCache[fd.GetName()]
	if !ok {
		f, err = toFileDescriptorLocked(fd, r)
		if err != nil {
			return nil, err
		}
		putCacheLocked(fd.GetName(), f)
	}

	ed := findEnum(f, path)
	enumCache[et] = ed
	return ed, nil
}

func getEnumFromCache(et reflect.Type) *EnumDescriptor {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	return enumCache[et]
}

func findEnum(fd *FileDescriptor, path []int) *EnumDescriptor {
	if len(path) == 1 {
		return fd.GetEnumTypes()[path[0]]
	}
	md := fd.GetMessageTypes()[path[0]]
	for _, i := range path[1 : len(path)-1] {
		md = md.GetNestedMessageTypes()[i]
	}
	return md.GetNestedEnumTypes()[path[len(path)-1]]
}

// LoadFieldDescriptorForExtension loads the field descriptor that corresponds to the given
// extension description.
func LoadFieldDescriptorForExtension(ext *proto.ExtensionDesc) (*FieldDescriptor, error) {
	return loadFieldDescriptorForExtension(ext, nil)
}

func loadFieldDescriptorForExtension(ext *proto.ExtensionDesc, r *ImportResolver) (*FieldDescriptor, error) {
	file, err := loadFileDescriptor(ext.Filename, r)
	if err != nil {
		return nil, err
	}
	field, ok := file.FindSymbol(ext.Name).(*FieldDescriptor)
	// make sure descriptor agrees with attributes of the ExtensionDesc
	if !ok || !field.IsExtension() || field.GetOwner().GetFullyQualifiedName() != proto.MessageName(ext.ExtendedType) ||
		field.GetNumber() != ext.Field {
		return nil, fmt.Errorf("file descriptor contained unexpected object with name %s", ext.Name)
	}
	return field, nil
}
