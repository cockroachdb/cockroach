workspace(name = "cockroach")

# Load the thing that lets us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Load go bazel tools.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "2697f6bc7c529ee5e6a2d9799870b9ec9eaeb3ee7d70ed50b87a2c2c97e13d9e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.23.8/rules_go-v0.23.8.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.23.8/rules_go-v0.23.8.tar.gz",
    ],
)

# Load gazelle.
http_archive(
    name = "bazel_gazelle",
    sha256 = "cdb02a887a7187ea4d5a27452311a75ed8637379a1287d8eeb952138ea485f7d",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.21.1/bazel-gazelle-v0.21.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.21.1/bazel-gazelle-v0.21.1.tar.gz",
    ],
)

# Load go rules.
load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

# Load protobuf depedency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_google_protobuf",
    commit = "09745575a923640154bcf307fba8aedff47f240a",
    # commit = "fbafa9842933bc547fbe0dbca9c882f37ff3a526",
    remote = "https://github.com/protocolbuffers/protobuf",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Load up go depenencies (the stuff under vendor).
#
#   gazelle update-repos -from_file=go.mod -to_macro=dependencies.bzl%go_deps, or
#   bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro=dependencies.bzl%go_deps

load("//:dependencies.bzl", "go_deps")

# gazelle:repository_macro dependencies.bzl%go_deps
go_deps()
