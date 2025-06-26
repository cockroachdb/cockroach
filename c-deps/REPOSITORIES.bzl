load(":archived.bzl", "archived_cdep_repository")

# c-deps repository definitions.

# Define the repositories for each of our c-dependencies. BUILD_ALL_CONTENT is
# a shorthand to glob over all checked-in files.
BUILD_ALL_CONTENT = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

# Each of these c-dependencies map to one or more library definitions in the
# top-level BUILD.bazel. Library definitions will list the following
# definitions as sources.

# This is essentially the same above, we elide a generated file to avoid
# permission issues when building jemalloc within the bazel sandbox.
BUILD_JEMALLOC_CONTENT = """filegroup(name = "all", srcs = glob(["**"], exclude=["configure"]), visibility = ["//visibility:public"])"""

# We do need to add native as new_local_repository is defined in Bazel core.
def c_deps():
    native.new_local_repository(
        name = "geos",
        path = "c-deps/geos",
        build_file_content = BUILD_ALL_CONTENT,
    )
    native.new_local_repository(
        name = "jemalloc",
        path = "c-deps/jemalloc",
        build_file_content = BUILD_JEMALLOC_CONTENT,
    )
    native.new_local_repository(
        name = "krb5",
        path = "c-deps/krb5",
        build_file_content = BUILD_ALL_CONTENT,
    )
    native.new_local_repository(
        name = "proj",
        path = "c-deps/proj",
        build_file_content = BUILD_ALL_CONTENT,
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linux",
        sha256 = "b1f66ed0dad1d05037b540e543680c4013739f87a3ecd2726425ba9b488c7561",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "5757c2aed7b45dc68e3c72b9a4ef611a74c9d1859ba670e63d5a81a2e0ea64aa",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "c171ae3c4ed091e566f1c3bacc91b025242ad22970461a87245c780934240c07",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "86156882ac66841d8ede581d441f331582354e9cf6c7b62699c4669a8cda4c1b",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "2dd4d906c15029b874576c0aaa6eaee04cedc9a20fdbd22abe64440ddb473475",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "0f48b961ad3332a066eecbe759ee879561455e694a5b38d9f5ba0fa96e25cfad",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "36f0c4881c89dea7630c946ef258446830d29d4082bf8ba2fd75124f3f822542",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "6a4d8ca691cafc0906e6b8c1945223f05ee25fda537131d6e8346942d8896122",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "55088072ee49c948f38f6bbf2bb38d629aec66e72e1cff7c9e5440428280ffd9",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "de256942fb37a818389f9c7c11da3eed105d34fcdd1012a2ca31e41acb1916a1",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "84f0c3218994bc88fe27bd8a48180b50f3c6b26ff0ebc2c4741f111f46d2a327",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "ecd6ea979ccc6f96464e3ecf28cc6b0dd2556862791a48d1ab4851d672d0322e",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "fcfbcf0e39bb8b150cf432e82a9c84d24135e4ebab906dbc03bda2dc9d8215f3",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "7bcd450bb15c7f9e9332f0dfd0cff895ffbbd26b21d7a52d1d67e38bd5910523",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "bee18cc0640c8f0bd204f649e60e04066709ab6b282c6d4e63eebee7956b70ca",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "75e66ae5995a413ba3f37a479efd5673cac4e2821070976e12541692cda2a3f9",
    )
