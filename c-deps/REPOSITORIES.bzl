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
        sha256 = "7b4238a4f96cc9b142a8da4478fa11e43c99203fd7a39cee5d03ccf9e8d924a0",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "e0919963802b45b49a5f806680936ffa99dd5da5cf6b6607518f76db225b3d02",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "7fe3b0f9816ec9c9d52bc2dc6e0e3c69dff34cec9b4530878852ef1c890815ad",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "417aa26608b9b474f9a84d846543570de0f8b20a355b3a171676780083bacfba",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "4e84cbe929e507a9a4a48df50882d4bc25164d1fff48331c748fbeb1bac80171",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "9c0054035393ac40add5516e25e27745c9cdb525a9025e117b2325baa3fc7f88",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "aa1db8812f58446e09b047b7d138ec0f624bfa5737f46056a6cbc9136ac966d1",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "64a40b15c42210ff3853b8e445e8172f450e1c6fcb905e018f81202e2d435cfd",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "c1b8a99e5fd26fb9b33ffc5f4fddcabc733db7c495e896937ccabbf8c4048e6d",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "cc7d23cdddfc396606846592cf9e1c6b4397535286e7b96d294c2137bd44b629",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "17db8505fdff25224f7378a2c2b772d15cbd5dde0f29fcfa44a0d655435e2a89",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "937e8c6f1e9668531bd9940865de7b53918268d484a60c2798c0cea32cea5834",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "99d4af4479021dde5b1d945ea2d9a301245ed55a50264410944544d3c48fb76f",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "27f2c0e30b365c77ee6ec312be3e4272521fd1ae01631d2571a5292c39dc2cd0",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "4067b8f664313f392d7bcca28a4dfcf2c27c876178021a73c1baf05282b8cd0e",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "b94d19b4d30a85d9b79f6f24138e00aca557d2b5a760c74e2ad19f478f6e68d9",
    )
