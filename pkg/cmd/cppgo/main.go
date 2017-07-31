package main

import (
	"fmt"
	"path"
	"reflect"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var types = make(map[reflect.Type]bool)
var skipTypes = make(map[reflect.Type]bool)
var defTypes []reflect.Type

func typeName(t reflect.Type) string {
	if skipTypes[t] {
		return ""
	}

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
		return "GoString"
	case reflect.Array:
		elemName := typeName(t.Elem())
		if elemName == "" {
			return ""
		}
		return fmt.Sprintf("GoArray<%s,%d>", elemName, t.Len())
	case reflect.Slice:
		sep := ""
		elemName := typeName(t.Elem())
		if elemName == "" {
			return ""
		}
		if strings.HasSuffix(elemName, ">") {
			sep = " "
		}
		return fmt.Sprintf("GoSlice<%s%s>", elemName, sep)
	case reflect.Ptr:
		return fmt.Sprintf("GoPointer<%s >", typeName(t.Elem()))
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

func genDecls(t reflect.Type) {
	if t.Kind() == reflect.Ptr {
		genDecls(t.Elem())
		return
	}
	if t.Kind() != reflect.Struct {
		return
	}
	if types[t] || skipTypes[t] {
		return
	}
	types[t] = true
	defTypes = append(defTypes, t)

	fmt.Printf("\n")
	fmt.Printf("namespace %s { class %s; }\n", path.Base(t.PkgPath()), t.Name())

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		switch f.Type.Kind() {
		case reflect.Struct:
			genDecls(f.Type)
		case reflect.Ptr, reflect.Array, reflect.Slice:
			genDecls(f.Type.Elem())
		}
	}

	fmt.Printf("\n")
	fmt.Printf("namespace %s {\n", path.Base(t.PkgPath()))
	fmt.Printf("class %s {\n", t.Name())
	fmt.Printf(" public:\n")
	fmt.Printf("  enum { Size = %d };\n", t.Size())
	fmt.Printf("\n")
	fmt.Printf(" public:\n")

	var constructor bool
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tName := typeName(f.Type)
		if tName == "" {
			fmt.Printf("  // TODO: %s %s[%s] %d\n",
				accessorName(f.Name), f.Type, f.Type.Kind(), f.Offset)
			continue
		}
		if !constructor {
			constructor = true
			fmt.Printf("  %s(const void *data)\n", t.Name())
			fmt.Printf("    : data_(data) {\n")
			fmt.Printf("  }\n")
			fmt.Printf("\n")
		}
		fmt.Printf("  %s %s() const;\n", tName, accessorName(f.Name))
	}
	if !constructor {
		fmt.Printf("  %s(const void *data) {\n", t.Name())
		fmt.Printf("  }\n")
	} else {
		fmt.Printf("\n private:\n")
		fmt.Printf("  const void *data_;\n")
	}
	fmt.Printf("};\n")
	fmt.Printf("}  // namespace %s\n", path.Base(t.PkgPath()))
}

func genDefs() {
	for _, t := range defTypes {
		fmt.Printf("\n")
		fmt.Printf("namespace %s {\n", path.Base(t.PkgPath()))
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tName := typeName(f.Type)
			if tName == "" {
				continue
			}

			var sep = ""
			switch f.Type.Kind() {
			case reflect.Array, reflect.Slice, reflect.Ptr:
				sep = " "
			}

			fmt.Printf("inline %s %s::%s() const {\n", tName, t.Name(), accessorName(f.Name))
			fmt.Printf("  return GoType<%s%s>::get(data_, %d);\n", tName, sep, f.Offset)
			fmt.Printf("}\n")
		}
		fmt.Printf("}  // namespace %s\n", path.Base(t.PkgPath()))
	}
}

func genPrologue() {
	fmt.Printf(`#ifndef CPPGO_H
#define CPPGO_H

#include <stdint.h>

template <typename T>
struct GoType {
  enum { Size = T::Size };
  typedef T Result;
  static T get(const void *data, int offset) {
    return T(reinterpret_cast<const uint8_t*>(data) + offset);
  }
};
`)

	for _, t := range []string{
		"bool", "int8_t", "int16_t", "int32_t", "int64_t",
		"uint8_t", "uint16_t", "uint32_t", "uint64_t", "uintptr_t",
		"float", "double"} {
		fmt.Printf(`
template <>
struct GoType<%s> {
  enum { Size = sizeof(%s) };
  typedef %s Result;
  static %s get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const %s*>(ptr);
  }
};
`, t, t, t, t, t)
	}

	fmt.Printf(`
class GoString {
 struct Header {
   const char *data;
   int64_t size;
 };

 public:
   enum { Size = sizeof(Header) };

 public:
  GoString(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  std::string as_string() const { return std::string(data(), size()); }
  const char* data() const { return hdr_->data; }
  int64_t size() const { return hdr_->size; }

 private:
  const Header* hdr_;
};

template <typename T>
class GoPointer {
 public:
  enum { Size = sizeof(void*) };

 public:
  GoPointer(const void *data)
    : data_(data) {
  }

  const void* get() const {
    return *reinterpret_cast<const uint8_t* const*>(data_);
  }
  operator bool() const {
    return get() != nullptr;
  }
  T operator*() const {
    return GoType<T>::get(get(), 0);
  }
  T operator->() const {
    return GoType<T>::get(get(), 0);
  }

 private:
  const void* data_;
};

template <typename C>
class GoIterator {
 public:
  typedef typename C::value_type value_type;
  typedef typename C::reference_type reference_type;
  typedef GoIterator<C> self_type;

 public:
  GoIterator(const C *c, int index)
    : container_(c),
      index_(index) {
  }
  GoIterator(const self_type& x)
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
class GoSlice {
  struct Header {
    const uint8_t *data;
    int64_t size;
    int64_t cap;
  };

 public:
  enum { Size = sizeof(Header) };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef GoIterator<GoSlice<T> > iterator;
  typedef iterator const_iterator;

 public:
  GoSlice(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  value_type operator[](int i) const {
    return GoType<T>::get(hdr_->data, i * GoType<T>::Size);
  }

  int64_t size() const { return hdr_->size; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }  

 private:
  const Header* hdr_;
};

template <typename T, int N>
class GoArray {
 public:
  enum { Size = N * GoType<T>::Size };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef GoIterator<GoArray<T, N> > iterator;
  typedef iterator const_iterator;

 public:
  GoArray(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  value_type operator[](int i) const {
    return GoType<T>::get(data_, i * GoType<T>::Size);
  }

  int64_t size() const { return N; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }  

 private:
  const uint8_t *data_;
};
`)
}

func genEpilogue() {
	fmt.Printf(`
#endif  // CPPGO_H
`)
}

func main() {
	skipTypes[reflect.TypeOf(tracing.RecordedSpan{})] = true

	genPrologue()
	genDecls(reflect.TypeOf(roachpb.BatchRequest{}))
	genDecls(reflect.TypeOf(roachpb.BatchResponse{}))
	genDefs()
	genEpilogue()
}
