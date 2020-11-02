# Define the top level namespace. This lets everything be addressable using
# `@cockroach//...`.
workspace(name = "cockroach")

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "ac03931e56c3b229c145f1a8b2a2ad3e8d8f1af57e43ef28a26123362a1e3c7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.24.4/rules_go-v0.24.4.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.24.4/rules_go-v0.24.4.tar.gz",
    ],
)

# Load the go dependencies and invoke them.
load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")
go_rules_dependencies()
go_register_toolchains()

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
#
# TODO(irfansharif): Point to a proper bazelle-gazelle release once
# https://github.com/bazelbuild/bazel-gazelle/pull/933 lands upstream.
git_repository(
    name = "bazel_gazelle",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
    commit = "493b9adf67665beede36502c2094496af9f245a3",
)

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
gazelle_dependencies()

# Load the protobuf depedency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
#
# TODO(irfansharif): We're not yet using this. We'll eventually need to when we
# try to generate proto files through bazel.
git_repository(
    name = "com_google_protobuf",
    commit = "09745575a923640154bcf307fba8aedff47f240a",
    remote = "https://github.com/protocolbuffers/protobuf",
)
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

# Load up cockroachdb's go dependencies (the ones listed under go.mod). The
# `DEPS.bzl` file is kept upto date using the following:
#
#   bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro=DEPS.bzl%go_deps
#
# gazelle:repository_macro DEPS.bzl%go_deps
load("//:DEPS.bzl", "go_deps")
go_deps()

# Load the bazel utility that lets us build C/C++ projects using cmake/make/etc.
#
# TODO(irfansharif): Point to an upstream SHA once it picks up Oliver's changes
# that add autoconf support.
git_repository(
   name = "rules_foreign_cc",
   commit = "605c77171f20840464301d7d01d6cd9e3a982888",
   remote = "https://github.com/otan-cockroach/rules_foreign_cc",
)
load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")
rules_foreign_cc_dependencies()

# Define the repositories for each of our c-dependencies. BUILD_ALL_CONTENT is
# a shorthand to glob over all checked-in files.
BUILD_ALL_CONTENT = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

# Each of these c-dependencies map to one or more library definitions in the
# top-level BUILD.bazel. Library definitions will list the following
# definitions as sources. For certain libraries (like `libroachccl`), the
# repository definitions are also listed as tool dependencies. This is because
# building those libraries require certain checked out repositories being
# placed relative to the source tree of the library itself.

new_local_repository(
    name = "libroach",
    path = "c-deps/libroach",
    build_file_content = BUILD_ALL_CONTENT,
)

new_local_repository(
  name = "proj",
  path = "c-deps/proj",
  build_file_content = BUILD_ALL_CONTENT,
)

# For c-deps/protobuf, we elide a checked in generated file. Already generated
# files are read-only in the bazel sandbox, so bazel is unable to regenerate
# the same files, which the build process requires it to do so.
BUILD_PROTOBUF_CONTENT = """filegroup(name = "all", srcs = glob(["**"], exclude=["src/google/protobuf/compiler/js/well_known_types_embed.cc"]), visibility = ["//visibility:public"])"""
new_local_repository(
   name = "protobuf",
   path = "c-deps/protobuf",
   build_file_content = BUILD_PROTOBUF_CONTENT,
)

# This is essentially the same above, we elide a generated file to avoid
# permission issues when building jemalloc within the bazel sandbox.
BUILD_JEMALLOC_CONTENT = """filegroup(name = "all", srcs = glob(["**"], exclude=["configure"]), visibility = ["//visibility:public"])"""
new_local_repository(
  name = "jemalloc",
  path = "c-deps/jemalloc",
  build_file_content = BUILD_JEMALLOC_CONTENT,
)
