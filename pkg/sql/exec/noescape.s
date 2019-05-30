// This empty assembly file has non-obvious side effects.
//
// 1. In go 1.11, the existence of an assembly file makes it
// possible to declare a function with no body. This in turn is
// needed to use the go:linkname directive to refer to functions
// from another package. This is the reason this file exists and
// it should go away when we require go 1.12.
//
// 2. Assembly files may cause GCC to mark the binary
// as requiring an executable stack. This is a security risk. The
// magic below instructs GCC to keep the stack non-executable.
//
// For reasons that are not understood, point 2 only applies in
// some packages (I think it's related to whether cgo is also used
// in the package). In packages where this is not true, the
// .s file is not run through the preprocessor, so we can't
// use ifdef guards. Since it doesn't appear to matter, we
// don't use the magic at all in those cases.
//
// References:
// https://wiki.ubuntu.com/SecurityTeam/Roadmap/ExecutableStacks
// https://github.com/cockroachdb/cockroach/issues/37885

// #if defined(__linux__) && defined(__ELF__)
// .section        .note.GNU-stack, "", %progbits
// #endif
