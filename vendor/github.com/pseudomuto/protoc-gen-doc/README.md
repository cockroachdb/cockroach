# protoc-gen-doc

[![Travis Build Status][travis-svg]][travis-ci]
[![codecov][codecov-svg]][codecov-url]
[![GoDoc][godoc-svg]][godoc-url]
[![Go Report Card][goreport-svg]][goreport-url]

This is a documentation generator plugin for the Google Protocol Buffers compiler (`protoc`). The plugin can generate
HTML, JSON, DocBook and Markdown documentation from comments in your `.proto` files.

It supports proto2 and proto3, and can handle having both in the same context (see [examples](examples/) for proof).

## Installation

There is a Docker image available (`docker pull pseudomuto/protoc-gen-doc`) that has everything you need to generate
documentation from your protos.

If you'd like to install this locally, you can `go get` it.

`go get -u github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc`

## Invoking the Plugin

The plugin is invoked by passing the `--doc_out`, and `--doc_opt` options to the `protoc` compiler. The option has the
following format:

    --doc_opt=<FORMAT>|<TEMPLATE_FILENAME>,<OUT_FILENAME>

The format may be one of the built-in ones ( `docbook`, `html`, `markdown` or `json`)
or the name of a file containing a custom [Go template][gotemplate].

### Using the Docker Image (Recommended)

The docker image has two volumes: `/out` and `/protos` which are the directory to write the documentation to and the
directory containing your proto files.

You could generate HTML docs for the examples by running the following:

```
docker run --rm \
  -v $(pwd)/examples/doc:/out \
  -v $(pwd)/examples/proto:/protos \
  pseudomuto/protoc-gen-doc
```

By default HTML documentation is generated in `/out/index.html` for all `.proto` files in the `/protos` volume. This can
be changed by passing the `--doc_opt` parameter to the container.

For example, to generate Markdown for all the examples:

```
docker run --rm \
  -v $(pwd)/examples/doc:/out \
  -v $(pwd)/examples/proto:/protos \
  pseudomuto/protoc-gen-doc --doc_opt=markdown,docs.md
```

You can also generate documentation for a single file. This can be done by passing the file(s) to the command:

```
docker run --rm \
  -v $(pwd)/examples/doc:/out \
  -v $(pwd)/examples/proto:/protos \
  pseudomuto/protoc-gen-doc --doc_opt=markdown,docs.md /protos/Booking.proto [OPTIONALLY LIST MORE FILES]
```

You can also exclude proto files that match specific path expressions. This is done by passing a second option delimited by `:`.
For example, you can pass any number of comma separated patterns as the second option:

```
docker run --rm \
  -v $(pwd)/examples/doc:/out \
  -v $(pwd)/examples/proto:/protos \
  pseudomuto/protoc-gen-doc --doc_opt=:google/*,somepath/*
```

_**Remember**_: Paths should be from within the container, not the host!

> NOTE: Due to the way wildcard expansion works with docker you cannot use a wildcard path (e.g. `protos/*.proto`) in
the file list. To get around this, if no files are passed, the container will generate docs for `protos/*.proto`, which
can be changed by mounting different volumes.

### Simple Usage

For example, to generate HTML documentation for all `.proto` files in the `proto` directory into `doc/index.html`, type:

    protoc --doc_out=./doc --doc_opt=html,index.html proto/*.proto

The plugin executable must be in `PATH` for this to work. 

### Using a precompiled binary

Alternatively, you can specify a pre-built/not in `PATH` binary using the `--plugin` option.

    protoc \
      --plugin=protoc-gen-doc=./protoc-gen-doc \
      --doc_out=./doc \
      --doc_opt=html,index.html \
      proto/*.proto

### With a Custom Template

If you'd like to use your own template, simply use the path to the template file rather than the type.

    protoc --doc_out=./doc --doc_opt=/path/to/template.tmpl,index.txt proto/*.proto

For information about the available template arguments and functions, see [Custom Templates][custom]. If you just want
to customize the look of the HTML output, put your CSS in `stylesheet.css` next to the output file and it will be picked
up.

## Writing Documentation

Messages, Fields, Services (and their methods), Enums (and their values), Extensions, and Files can be documented.
Generally speaking, comments come in 2 forms: leading and trailing.

**Leading comments**

Leading comments can be used everywhere.

```protobuf
/**
 * This is a leading comment for a message
*/
message SomeMessage {
  // this is another leading comment
  string value = 1;
}
```

> NOTE: File level comments should be leading comments on the syntax directive.

**Trailing comments**

Fields, Service Methods, Enum Values and Extensions support trailing comments.

```protobuf
enum MyEnum {
  DEFAULT = 0; // the default value
  OTHER   = 1; // the other value
}
```

**Excluding comments**

If you want to have some comment in your proto files, but don't want them to be part of the docs, you can simply prefix
the comment with `@exclude`. 

Example: include only the comment for the `id` field

```protobuf
/**
 * @exclude
 * This comment won't be rendered
 */
message ExcludedMessage {
  string id   = 1; // the id of this message.
  string name = 2; // @exclude the name of this message

  /* @exclude the value of this message. */
  int32 value = 3;
}
```

Check out the [example protos](examples/proto) to see all the options.

## Output Example

With the input `.proto` files

* [Booking.proto](examples/proto/Booking.proto)
* [Customer.proto](examples/proto/Customer.proto)
* [Vehicle.proto](examples/proto/Vehicle.proto)

the plugin gives the output

* [Markdown](examples/doc/example.md)
* [HTML][html_preview]
* [DocBook](examples/doc/example.docbook)
* [JSON](examples/doc/example.json)

Check out the `examples` task in the [Makefile](Makefile) to see how these were generated.

[gotemplate]:
    https://golang.org/pkg/text/template/
    "Template - The Go Programming Language"
[custom]:
    https://github.com/pseudomuto/protoc-gen-doc/wiki/Custom-Templates
    "Custom templates instructions"
[html_preview]:
    https://rawgit.com/pseudomuto/protoc-gen-doc/master/examples/doc/example.html
    "HTML Example Output"
[travis-svg]:
    https://travis-ci.org/pseudomuto/protoc-gen-doc.svg?branch=master
    "Travis CI build status SVG"
[travis-ci]:
    https://travis-ci.org/pseudomuto/protoc-gen-doc
    "protoc-gen-doc at Travis CI"
[codecov-svg]: https://codecov.io/gh/pseudomuto/protoc-gen-doc/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/pseudomuto/protoc-gen-doc
[godoc-svg]: https://godoc.org/github.com/pseudomuto/protoc-gen-doc?status.svg
[godoc-url]: https://godoc.org/github.com/pseudomuto/protoc-gen-doc
[goreport-svg]: https://goreportcard.com/badge/github.com/pseudomuto/protoc-gen-doc
[goreport-url]: https://goreportcard.com/report/github.com/pseudomuto/protoc-gen-doc
