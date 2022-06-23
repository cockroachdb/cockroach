// Package windows contains various Windows system call.
package windows

// Use "go generate -v -x ." to generate the source.

// Add -trace to enable debug prints around syscalls.
//go:generate go run $GOROOT/src/syscall/mksyscall_windows.go -systemdll=true -output zsyscall_windows.go syscall_windows.go
//go:generate go run fix_generated.go -input zsyscall_windows.go
