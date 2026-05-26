# Introduction

The codec package provides a way of serializing and deserializing data structures, with YAML,
that contain interfaces (referred to as dynamic types in this package)

For example:
``` go
struct Foo {
  Bar interface{}
}
```

Typically, this would serialize without incident, using the standard YAML
library, and whichever concrete data `Bar` holds will be serialized.


However, challenges emerge during deserialization back to the original
structure. Without type information, the decoder cannot determine what concrete
type `Bar` originally held, resulting in deserialization to generic containers
like `map[string]interface{}`.

Ideally, we want to preserve the original type information, enabling us to
maintain the polymorphic behavior associated with the original interface
implementation.

# Usage

Consider the following example types:
``` go
type (
	Animal interface {
		codec.DynamicType // Requierd for dynamic type serialization
		Speak() string
	}

	Dog struct {
		Name string
	}

	Cat struct {
		Name string
	}
)

func (d Dog) Speak() string {
	return d.Name + " woofs"
}

func (c Cat) Speak() string {
	return c.Name + " meows"
}
```

The `codec.DynamicType` should be added to any type that is to be serialized
as a dynamic type.

## Registration

In order for the codec package decoder to recognize the types, they must be
registered and implement the `codec.DynamicType`. All concrete types that
implement `codec.DynamicType` must be registered.

A custom `TypeName` can be provided for each type, but it's preferred to use the
`codec.ResolveTypeName` function to generate a unique name for each type.

``` go
func init() {
	codec.Register(new(Dog))
	codec.Register(new(Cat))
}

func (d Dog) GetTypeName() codec.TypeName {
	return codec.ResolveTypeName(d)
}

func (c Cat) GetTypeName() codec.TypeName {
	return codec.ResolveTypeName(c)
}
```

## Encoding and decoding

Example of a structure ready to be serialized:
``` go
type struct Data {
	animals         codec.ListWrapper[Animal]
	favouriteAnimal codec.Wrapper[Animal]
	extinctAnimal   codec.Wrapper[*Animal] // Pointers can be used as well
}
```

The structure can then be serialized and deserialized as usual:
``` go
var data Data
// ...
buf, err := yaml.Marshal(data)
// ...
dec := yaml.NewDecoder(bytes.NewReader(buf))
dec.KnownFields(true)
err = dec.Decode(&data)
```

### Convenience methods

The wrappers provide a convenient `Get` method that retrieves the underlying
value with proper type information preserved, leveraging Go's generics system to
ensure type safety without manual type assertions.

``` go
var animal Animal = data.favouriteAnimal.Get()
var animals []Animal = data.animals.Get()
data.extinctAnimal.Get().Speak()
```
