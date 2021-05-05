# Define the top level namespace. This lets everything be addressable using
# `@cockroach//...`.
workspace(
    name = "cockroach",
    managed_directories = {"@npm": ["node_modules"]},
)

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
git_repository(
    name = "io_bazel_rules_go",
    commit = "2fe8a6256c818840cc9a10cf3f366d8410437245",
    remote = "https://github.com/cockroachdb/rules_go",
)

# Like the above, but for nodeJS.
http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "6142e9586162b179fdd570a55e50d1332e7d9c030efd853453438d607569721d",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/3.0.0/rules_nodejs-3.0.0.tar.gz"],
)

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
git_repository(
    name = "bazel_gazelle",
    commit = "e9091445339de2ba7c01c3561f751b64a7fab4a5",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
)

# Override the location of some libraries; otherwise, rules_go will pull its own
# versions. Note these declarations must occur BEFORE the call to
# go_rules_dependencies().
#
# Ref: https://github.com/bazelbuild/rules_go/blob/master/go/dependencies.rst#overriding-dependencies
load("@bazel_gazelle//:deps.bzl", "go_repository")

go_repository(
    name = "org_golang_x_sys",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/sys",
    sum = "h1:cdsMqa2nXzqlgs183pHxtvoVwU7CyzaCTAUOg94af4c=",
    version = "v0.0.0-20210503173754-0981d6026fa6",
)

go_repository(
    name = "org_golang_x_tools",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/tools",
    sum = "h1:po9/4sTYwZU9lPhi1tOrb4hCv3qrhiQ77LZfGa2OjwY=",
    version = "v0.1.0",
)

go_repository(
    name = "org_golang_x_xerrors",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/xerrors",
    sum = "h1:go1bK/D/BFZV2I8cIQd1NKEZ+0owSTG1fDTci4IqFcE=",
    version = "v0.0.0-20200804184101-5ec99f83aff1",
)

go_repository(
    name = "com_github_gogo_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "github.com/gogo/protobuf",
    patch_args = ["-p1"],
    patches = [
        "@cockroach//build/patches:com_github_gogo_protobuf.patch",
    ],
    sum = "h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=",
    version = "v1.3.2",
)

go_repository(
    name = "com_github_golang_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "github.com/golang/protobuf",
    patch_args = ["-p1"],
    patches = [
        "@cockroach//build/patches:com_github_golang_protobuf.patch",
    ],
    sum = "h1:ROPKBNFfQgOUMifHyP+KYbvpjbdoFNs+aK7DXlji0Tw=",
    version = "v1.5.2",
)

go_repository(
    name = "org_golang_google_genproto",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/genproto",
    sum = "h1:PDIOdWxZ8eRizhKa1AAvY53xsvLB1cWorMjslvY3VA8=",
    version = "v0.0.0-20200825200019-8632dd797987",
)

go_repository(
    name = "in_gopkg_yaml_v2",
    build_file_proto_mode = "disable_global",
    importpath = "gopkg.in/yaml.v2",
    replace = "github.com/cockroachdb/yaml",
    sum = "h1:EqoCicA1pbWWDGniFxhTElh2hvui7E7tEvuBNJSDn3A=",
    version = "v0.0.0-20180705215940-0e2822948641",
)

# Load the go dependencies and invoke them.
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(go_version = "1.15.11")

# Configure nodeJS.
load("@build_bazel_rules_nodejs//:index.bzl", "yarn_install")

yarn_install(
    name = "npm",
    package_json = "//pkg/ui:package.json",
    yarn_lock = "//pkg/ui:yarn.lock",
)

# NB: @bazel_skylib comes from go_rules_dependencies().
load("@bazel_skylib//lib:versions.bzl", "versions")

versions.check(minimum_bazel_version = "3.5.0")

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

# Load the protobuf depedency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
git_repository(
    name = "com_google_protobuf",
    commit = "9b23a34c7275aa0ceb2fc69ed1ae6737b34656a3",
    remote = "https://github.com/cockroachdb/protobuf",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Load up cockroachdb's go dependencies (the ones listed under go.mod). The
# `DEPS.bzl` file is kept up to date using the `update-repos` Gazelle command
# (see `make bazel-generate`).
#
# gazelle:repository_macro DEPS.bzl%go_deps
load("//:DEPS.bzl", "go_deps")

go_deps()

# Loading c-deps third party dependencies.
load("//c-deps:REPOSITORIES.bzl", "c_deps")

c_deps()

# Load the bazel utility that lets us build C/C++ projects using
# cmake/make/etc. We point to our fork which adds BSD support
# (https://github.com/bazelbuild/rules_foreign_cc/pull/387) and sysroot
# support (https://github.com/bazelbuild/rules_foreign_cc/pull/532).
#
# TODO(irfansharif): Point to an upstream SHA once maintainers pick up the
# aforementioned PRs.
git_repository(
    name = "rules_foreign_cc",
    commit = "6127817283221408069d4ae6765f2d8144f09b9f",
    remote = "https://github.com/cockroachdb/rules_foreign_cc",
)

load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

# Load custom toolchains.
load("//build:toolchains/REPOSITORIES.bzl", "toolchain_dependencies")

toolchain_dependencies()
