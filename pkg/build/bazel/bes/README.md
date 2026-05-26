The content in this directory is vendored from the upstream bazel repository at
github.com/bazelbuild/bazel.

In particular the build_event_stream.proto file is found in the
`src/main/java/com/google/devtools/build/lib/buildeventstream/proto` subdirectory,
and the remaining files are found in the `src/main/protobuf` subdirectory.

We split the files from that `src/main/protobuf` subdirectory into multiple
directories as we vendor them, because `gazelle` has difficulty with multiple
.proto files in the same package (directory) belonging to different `proto` or
`golang` packages.