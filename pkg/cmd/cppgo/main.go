package main

import (
	"fmt"
	"log"
	"path"
	"reflect"
	"sort"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

var types = make(map[reflect.Type]bool)
var declTypes = make(map[reflect.Type]bool)
var namespaces = make(map[string]*namespace)

type namespace struct {
	name  string
	deps  map[*namespace]bool
	types []reflect.Type
	seen  map[reflect.Type]bool
}

func typeName(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.Int8:
		return "int8_t"
	case reflect.Int16:
		return "int16_t"
	case reflect.Int32:
		return "int32_t"
	case reflect.Int, reflect.Int64:
		return "int64_t"
	case reflect.Uint8:
		return "uint8_t"
	case reflect.Uint16:
		return "uint16_t"
	case reflect.Uint32:
		return "uint32_t"
	case reflect.Uint, reflect.Uint64:
		return "uint64_t"
	case reflect.Uintptr:
		return "uintptr_t"
	case reflect.Float32:
		return "float"
	case reflect.Float64:
		return "double"
	case reflect.String:
		return "String"
	case reflect.Array:
		elemName := typeName(t.Elem())
		if elemName == "" {
			return ""
		}
		return fmt.Sprintf("Array<%s,%d>", elemName, t.Len())
	case reflect.Slice:
		elemName := typeName(t.Elem())
		if elemName == "" {
			return ""
		}
		return fmt.Sprintf("Slice<%s>", elemName)
	case reflect.Ptr:
		elemName := typeName(t.Elem())
		if elemName == "" {
			return ""
		}
		return fmt.Sprintf("Pointer<%s>", elemName)
	case reflect.Struct:
		pkgPath := t.PkgPath()
		if pkgPath == "" {
			return t.Name()
		}
		return path.Base(pkgPath) + "::" + t.Name()
	default:
		// TODO(peter): unsupported
		//   reflect.Complex64
		//   reflect.Complex128
		//   reflect.Chan
		//   reflect.Func
		//   reflect.Interface
		//   reflect.Map
		//   reflect.UnsafePointer
		return ""
	}
}

var cppKeywords = map[string]bool{
	"delete": true,
	"export": true,
	"inline": true,
}

func sanitizeName(s string) string {
	if cppKeywords[s] {
		return s + "_"
	}
	return s
}

func accessorName(s string) string {
	out := make([]rune, 0, len(s))
	prevUpper := false
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 && !prevUpper {
				out = append(out, '_')
			}
			r = unicode.ToLower(r)
			prevUpper = true
		} else {
			prevUpper = false
		}
		out = append(out, r)
	}
	return sanitizeName(string(out))
}

func walk(t reflect.Type) *namespace {
	switch t.Kind() {
	case reflect.Ptr, reflect.Array, reflect.Slice:
		return walk(t.Elem())
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	name := path.Base(t.PkgPath())
	n := namespaces[name]
	if n == nil {
		n = &namespace{
			name: name,
			deps: make(map[*namespace]bool),
			seen: make(map[reflect.Type]bool),
		}
		namespaces[name] = n
	}
	if n.seen[t] {
		return n
	}
	n.seen[t] = true
	n.types = append(n.types, t)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			// Unexported field.
			continue
		}
		switch f.Type.Kind() {
		case reflect.Struct, reflect.Ptr, reflect.Array, reflect.Slice:
			if d := walk(f.Type); d != nil && d != n {
				n.deps[d] = true
			}
		}
	}

	return n
}

func gen() {
	sorted := make([]*namespace, 0, len(namespaces))
	for _, n := range namespaces {
		sorted = append(sorted, n)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].name < sorted[j].name
	})
	visited := make(map[*namespace]bool)
	for _, n := range sorted {
		n.gen(visited)
	}
}

func (n *namespace) gen(visited map[*namespace]bool) {
	if visited[n] {
		return
	}
	visited[n] = true

	deps := make([]*namespace, 0, len(n.deps))
	for d := range n.deps {
		deps = append(deps, d)
	}
	sort.Slice(deps, func(i, j int) bool {
		return deps[i].name < deps[j].name
	})
	for _, d := range deps {
		d.gen(visited)
	}

	fmt.Printf("namespace %s {\n", n.name)
	for _, t := range n.types {
		n.genType(t)
	}
	fmt.Printf("\n} // namespace %s\n\n", n.name)
}

func (n *namespace) genType(t reflect.Type) {
	switch t.Kind() {
	case reflect.Ptr, reflect.Array, reflect.Slice:
		n.genType(t.Elem())
		return
	}
	if t.Kind() != reflect.Struct {
		return
	}
	if types[t] {
		if !declTypes[t] {
			declTypes[t] = true
			fmt.Printf("\n")
			if namespace := path.Base(t.PkgPath()); n.name != namespace {
				log.Fatalf("unexpected namespace: %s != %s", n.name, namespace)
			}
			fmt.Printf("class %s;\n", t.Name())
		}
		return
	}
	types[t] = true

	if namespace := path.Base(t.PkgPath()); n.name != namespace {
		log.Fatalf("unexpected namespace: %s != %s", n.name, namespace)
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			// Unexported field.
			continue
		}
		switch f.Type.Kind() {
		case reflect.Struct, reflect.Ptr, reflect.Array, reflect.Slice:
			n.genType(f.Type)
		}
	}

	declTypes[t] = true

	fmt.Printf("\n")
	fmt.Printf("class %s {\n", t.Name())
	fmt.Printf(" public:\n")
	fmt.Printf("  enum { Size = %d };\n", t.Size())
	fmt.Printf("\n")
	fmt.Printf(" public:\n")

	var constructor bool
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			// Unexported field.
			fmt.Printf("  // UNEXPORTED: %s %s %d\n",
				accessorName(f.Name), f.Type, f.Offset)
			continue
		}
		tName := typeName(f.Type)
		if tName == "" {
			fmt.Printf("  // UNIMPLEMENTED: %s %s %d\n",
				accessorName(f.Name), f.Type, f.Offset)
			continue
		}
		if !constructor {
			constructor = true
			fmt.Printf("  %s(const void *data)\n", t.Name())
			fmt.Printf("    : data_(reinterpret_cast<const uint8_t*>(data)) {\n")
			fmt.Printf("  }\n")
			fmt.Printf("\n")
		}
		fmt.Printf("  %s %s() const {\n", tName, accessorName(f.Name))
		fmt.Printf("    return Type<%s>::get(data_ + %d);\n", tName, f.Offset)
		fmt.Printf("  }\n")
	}
	if !constructor {
		fmt.Printf("  %s(const void *data) {\n", t.Name())
		fmt.Printf("  }\n")
	} else {
		fmt.Printf("\n private:\n")
		fmt.Printf("  const uint8_t *data_;\n")
	}
	fmt.Printf("};\n")
}

func main() {
	fmt.Printf(prologue)
	walk(reflect.TypeOf(roachpb.BatchRequest{}))
	walk(reflect.TypeOf(roachpb.BatchResponse{}))
	gen()
	fmt.Printf(epilogue)
}

var prologue = `#ifndef CPPGO_H
#define CPPGO_H

#include <type_traits>
#include <stdint.h>
#include <rocksdb/slice.h>

namespace go {

template <typename T, typename E = void>
struct Type;

template <typename T>
struct Type<T, typename std::enable_if<std::is_class<T>::value>::type> {
  enum { Size = T::Size };
  static T get(const void *data) {
    return T(data);
  }
};

template <typename T>
struct Type<T, typename std::enable_if<std::is_arithmetic<T>::value>::type> {
  enum { Size = sizeof(T) };
  static T get(const void *data) {
    return *reinterpret_cast<const T*>(data);
  }
};

class String {
 struct Header {
   const char *data;
   int64_t size;
 };

 public:
   enum { Size = sizeof(Header) };

 public:
  String(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  std::string as_string() const { return std::string(data(), size()); }
  rocksdb::Slice as_slice() const { return rocksdb::Slice(data(), size()); }
  const char* data() const { return hdr_->data; }
  int64_t size() const { return hdr_->size; }

 private:
  const Header* hdr_;
};

template <typename T>
class Pointer {
 public:
  enum { Size = sizeof(void*) };

 public:
  Pointer(const void *data)
    : data_(data) {
  }

  const void* get() const {
    return *reinterpret_cast<const uint8_t* const*>(data_);
  }
  operator bool() const {
    return get() != nullptr;
  }
  T operator*() const {
    return Type<T>::get(get());
  }
  T operator->() const {
    return Type<T>::get(get());
  }

 private:
  const void* data_;
};

template <typename C>
class Iterator {
 public:
  typedef typename C::value_type value_type;
  typedef typename C::reference_type reference_type;
  typedef Iterator<C> self_type;

 public:
  Iterator(const C *c, int index)
    : container_(c),
      index_(index) {
  }
  Iterator(const self_type& x)
    : container_(x.container_),
      index_(x.index_) {
  }

  value_type operator*() const {
    return (*container_)[index_];
  }

  self_type operator++(int) {
    self_type t = *this;
    return ++t;
  }
  self_type& operator++() {
    ++index_;
    return *this;
  }
  self_type operator--(int) {
    self_type t = *this;
    return --*t;
  }
  self_type& operator--() {
    --index_;
    return *this;
  }

  bool operator==(const self_type& other) const {
    return container_ == other.container_ && index_ == other.index_;
  }
  bool operator!=(const self_type& other) const {
    return !(*this == other);
  }

 private:
  const C *container_;
  int index_;
};

template <typename T>
class Slice {
  struct Header {
    const uint8_t *data;
    int64_t size;
    int64_t cap;
  };

 public:
  enum { Size = sizeof(Header) };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef Iterator<Slice<T>> iterator;
  typedef iterator const_iterator;

 public:
  Slice(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  value_type operator[](int i) const {
    return Type<T>::get(hdr_->data + i * Type<T>::Size);
  }

  int64_t size() const { return hdr_->size; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }

 private:
  const Header* hdr_;
};

template <typename T, int N>
class Array {
 public:
  enum { Size = N * Type<T>::Size };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef Iterator<Array<T, N>> iterator;
  typedef iterator const_iterator;

 public:
  Array(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  value_type operator[](int i) const {
    return Type<T>::get(data_ + i * Type<T>::Size);
  }

  int64_t size() const { return N; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }

 private:
  const uint8_t *data_;
};

`

var epilogue = `} // namespace go

#endif  // CPPGO_H
`
