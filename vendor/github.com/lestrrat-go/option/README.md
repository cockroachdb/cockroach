# option

Base object for what I call the "Optional Parameters Pattern".

The beauty of this pattern is that you can achieve a method that can
take the following simple calling style

```
obj.Method(mandatory1, mandatory2)
```

or the following, if you want to modify its behavior with optional parameters

```
obj.Method(mandatory1, mandatory2, optional1, optional2, optional3)
```

Intead of the more clunky zero value for optionals style

```
obj.Method(mandatory1, mandatory2, nil, "", 0)
```

or the equally cluncky config object style, which requires you to create a
struct with `NamesThatLookReallyLongBecauseItNeedsToIncludeMethodNamesConfig	

```
cfg := &ConfigForMethod{
 Optional1: ...,
 Optional2: ...,
 Optional3: ...,
}
obj.Method(mandatory1, mandatory2, &cfg)
```

# SYNOPSIS 

This library is intended to be a reusable component to implement
a function with arguments that look like the following:

```
obj.Method(mandatory1, mandatory2, optional1, optional2, optional3, ...)
```

Internally, we just declare this method as follows:

```
func (obj *Object) Method(m1 Type1, m2 Type2, options ...Option) {
  ...
}
```

Option objects take two arguments, its identifier and the value it contains.
The identifier can be anything, but it's usually better to use a an unexported
empty struct so that only you have the ability to generate said option:

```
type identOptionalParamOne struct{}
type identOptionalParamTwo struct{}
type identOptionalParamThree struct{}

func WithOptionOne(v ...) Option {
	return option.New(identOptionalParamOne{}, v)
}
```

Then you can call the method we described above as

```
obj.Method(m1, m2, WithOptionOne(...), WithOptionTwo(...), WithOptionThree(...))
```

Options should be parsed in a code that looks somewhat like this

```
func (obj *Object) Method(m1 Type1, m2 Type2, options ...Option) {
  paramOne := defaultValueParamOne
  for _, option := range options {
    switch option.Ident() {
		case identOptionalParamOne{}:
      paramOne = option.Value().(...)
    }
  }
  ...
}
```

# Simple usage

Most of the times all you need to do is to declare the Option type as an alias
in your code:

```
package myawesomepkg

import "github.com/lestrrat-go/option"

type Option = option.Interface
```

Then you can start definig options like they are described in the SYNOPSIS section.

# Differentiating Options

When you have multiple methods and options, and those options can only be passed to
each one the methods, it's hard to see which options should be passed to which method.

```
func WithX() Option {}
func WithY() Option {}

// Now, which of WithX/WithY go to which method?
func (*Obj) Method1(options ...Option) {}
func (*Obj) Method2(options ...Option) {}
```

In this case the easiest way to make it obvious is to put an extra layer around
the options so that they have different types

```
type Method1Option interface {
  Option
  method1Option()
}

type method1Option struct { Option }
func (*method1Option) method1Option() {}

func WithX() Method1Option {
  return &methodOption{option.New(...)}
}

func (*Obj) Method1(options ...Method1Option) {}
```

This way the compiler knows if an option can be passed to a given method.
