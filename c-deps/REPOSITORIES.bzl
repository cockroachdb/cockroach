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
        sha256 = "8ef54e59cad3b364f89ef60fbb71e6f7bce0486871a78648bdc27141a9ef885c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "8b27cf99f729ae2060a60017ddc9dd3e300d65d750f2b5bc38ad7fd0b994872f",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "969802e2cf23b13d8da5f7005bedd58b79fec980d9316b788851623f720ffe8a",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "011a2797d92bdc9ae2ec0c0bf2b76b14a0ba57b52350b16eec8b8726bf1244f5",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "6a0259c5a129c67e09dcc2031fa0127d5204e05780b65d6721557bc77fdfb470",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "c16cd33bfe7ec5ddc49133cedbdd66f700fbeebda0b653c993e21cb16a8bd955",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "823c8198cd500f2cac64e1078f92468eae3994d15e762a5fb2ebe1e694675c74",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "16139138879174f764d99c7a6344c72d3ff86e6aa4f2e261753b985990dd48b7",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "8c7a049b0f2ded3793f0c4f659155f09e02ea657fbc066e9561adf1bdeab516b",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "38688f8b329fe989d082e61070ca4a3a7b1d4bfa79fd2c0a0b832a3c807aec04",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "a3480b7bf2062b310e91bc8b65b2c67cd5f948857efc9bb3c574e35e6a997cf5",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "b68943a6efb2ed42883bb10521264f0a4bfe0d7eb9f019eb47e4a7906673cff3",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "d9aabd5701dececc79d3a8ca30dd9fd94c0ad1bd342c90f2b7b1fdaa94b7459a",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "09848d2e4efff4b3984a733a5d913687c6c886b28b059a33be778802b9694675",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "896866f89a2bbe5e7f7289c9cb1a93256e77b96b03cdce052559c27b7ab9087e",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "dbddeb047a8baa8100701a66ce40b4054b2a256b5c88921b1f7dab450de09973",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "3b8be5bf316c21860b0bb9d2a11100b98107d43594629029e2d254cfe8d8772d",
    )
