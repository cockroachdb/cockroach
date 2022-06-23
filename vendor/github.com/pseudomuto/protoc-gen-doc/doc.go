// Package gendoc is a protoc plugin for generating documentation from your proto files.
//
// Typically this will not be used as a library, though nothing prevents that. Normally it'll be invoked by passing
// `--doc_out` and `--doc_opt` values to protoc.
//
// Example: generate HTML documentation
//
//     protoc --doc_out=. --doc_opt=html,index.html protos/*.proto
//
// Example: exclude patterns
//
//     protoc --doc_out=. --doc_opt=html,index.html:google/*,somedir/* protos/*.proto
//
// Example: use a custom template
//
//     protoc --doc_out=. --doc_opt=custom.tmpl,docs.txt protos/*.proto
//
// For more details, check out the README at https://github.com/pseudomuto/protoc-gen-doc
package gendoc
