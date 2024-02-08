# Any arguments passed to this script will be directly passed to Bazel. In
# addition to just calling Bazel, this script has the property that the Bazel
# server will be killed if we receive an interrupt.

bazel_shutdown() {
    bazel shutdown
}

trap bazel_shutdown EXIT

bazel "$@"
