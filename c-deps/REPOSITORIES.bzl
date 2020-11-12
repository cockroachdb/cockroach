# c-deps repository definitions.

# Define the repositories for each of our c-dependencies. BUILD_ALL_CONTENT is
# a shorthand to glob over all checked-in files.
BUILD_ALL_CONTENT = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

# Each of these c-dependencies map to one or more library definitions in the
# top-level BUILD.bazel. Library definitions will list the following
# definitions as sources. For certain libraries (like `libroachccl`), the
# repository definitions are also listed as tool dependencies. This is because
# building those libraries require certain checked out repositories being
# placed relative to the source tree of the library itself.

# For c-deps/protobuf, we elide a checked in generated file. Already generated
# files are read-only in the bazel sandbox, so bazel is unable to regenerate
# the same files, which the build process requires it to do so.
BUILD_PROTOBUF_CONTENT = """filegroup(name = "all", srcs = glob(["**"], exclude=["src/google/protobuf/compiler/js/well_known_types_embed.cc"]), visibility = ["//visibility:public"])"""

# This is essentially the same above, we elide a generated file to avoid
# permission issues when building jemalloc within the bazel sandbox.
BUILD_JEMALLOC_CONTENT = """filegroup(name = "all", srcs = glob(["**"], exclude=["configure"]), visibility = ["//visibility:public"])"""

# We do need to add native as new_local_repository is defined in Bazel core.
def c_deps():
    native.new_local_repository(
        name = "libroach",
        path = "c-deps/libroach",
        build_file_content = BUILD_ALL_CONTENT,
    )
    native.new_local_repository(
        name = "proj",
        path = "c-deps/proj",
        build_file_content = BUILD_ALL_CONTENT,
    )
    native.new_local_repository(
        name = "protobuf",
        path = "c-deps/protobuf",
        build_file_content = BUILD_PROTOBUF_CONTENT,
    )
    native.new_local_repository(
        name = "jemalloc",
        path = "c-deps/jemalloc",
        build_file_content = BUILD_JEMALLOC_CONTENT,
    )
