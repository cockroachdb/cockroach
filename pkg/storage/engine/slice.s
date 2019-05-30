// This empty assembly file has non-obvious side effects.
//
// 1. The go:linkname directive is only enabled for packages that
// contain assembly files; this is the reason this file exists.
//
// 2. Completely empty assembly files cause GCC to mark the binary
// as requiring an executable stack. This is a security risk. The
// magic below instructs GCC to keep the stack non-executable.
//
// References:
// https://wiki.ubuntu.com/SecurityTeam/Roadmap/ExecutableStacks
// https://github.com/cockroachdb/cockroach/issues/37885

#if defined(__linux__) && defined(__ELF__)
.section        .note.GNU-stack, "", %progbits
#endif
