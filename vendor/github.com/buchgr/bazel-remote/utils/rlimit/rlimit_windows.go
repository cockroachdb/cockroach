// +build windows

package rlimit

// bazel-remote does not work with windows, due to the way we interact
// with the filesystem. Let's try to give a reasonable compile-time
// error message to prevent windows users from wasting time trying.
var _ = __BAZEL_REMOTE_WINDOWS_BUILDS_ARE_NOT_SUPPORTED__

func Raise() {
}
