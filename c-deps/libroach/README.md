# Sources for libroach.

## Formatting

Code is formatted using [clang-format](https://clang.llvm.org/docs/ClangFormat.html).

To install:
* download [LLVM  5.0 for your OS/arch](http://releases.llvm.org/download.html#5.0.0)
* extract archive
* place the archive's `bin/clang-format` in your path

If you use a package manager, make sure it uses clang-format from LLVM 5.0 (`clang-format --version`).

To use, do one of:
* run `make c-deps-fmt` from the `cockroachdb/cockroach` repo
* add `clang-format -i <filename>` as a save hook in your editor.
