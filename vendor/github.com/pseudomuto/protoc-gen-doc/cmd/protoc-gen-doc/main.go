// protoc-gen-doc is used to generate documentation from comments in your proto files.
//
// It is a protoc plugin, and can be invoked by passing `--doc_out` and `--doc_opt` arguments to protoc.
//
// Example: generate HTML documentation
//
//     protoc --doc_out=. --doc_opt=html,index.html protos/*.proto
//
// Example: use a custom template
//
//     protoc --doc_out=. --doc_opt=custom.tmpl,docs.txt protos/*.proto
//
// For more details, check out the README at https://github.com/pseudomuto/protoc-gen-doc
package main

import (
	"github.com/pseudomuto/protokit"

	"log"
	"os"

	gendoc "github.com/pseudomuto/protoc-gen-doc"
	_ "github.com/pseudomuto/protoc-gen-doc/extensions/google_api_http" // imported for side effects
	_ "github.com/pseudomuto/protoc-gen-doc/extensions/lyft_validate"   // imported for side effects
	_ "github.com/pseudomuto/protoc-gen-doc/extensions/validator_field" // imported for side effects
)

func main() {
	if flags := ParseFlags(os.Stdout, os.Args); HandleFlags(flags) {
		os.Exit(flags.Code())
	}

	if err := protokit.RunPlugin(new(gendoc.Plugin)); err != nil {
		log.Fatal(err)
	}
}

// HandleFlags checks if there's a match and returns true if it was "handled"
func HandleFlags(f *Flags) bool {
	if !f.HasMatch() {
		return false
	}

	if f.ShowHelp() {
		f.PrintHelp()
	}

	if f.ShowVersion() {
		f.PrintVersion()
	}

	return true
}
