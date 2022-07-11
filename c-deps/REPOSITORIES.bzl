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
        sha256 = "c02edafb99b5a289f04e689731f5cab498b852f3557120e329fcb842e5282471",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "ea73589ba3b4f677e1554ab84d562bafeba0b1057b7fbd651a1bc8bfe0a361ee",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "53712526d4fcd4f13d019f0c9b789bbca13ba11cf3f1574fa9a1db576c82aecd",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "a25bfdadf958955e8559ac2e03d5a76748919a838be07afef00635cc774b66c4",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "a91006cc7b0e468feaf7b0b8ba0ce892568fc2ce21319edb843f9827a7025aba",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "76a6bbc84f73753bbde78d6ec02d4aded310c740c9ee90944de816ef11127905",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "83c5110940056cdad028ef7265a0a0f1602a1e578a67cbe5849104f1db31bfa0",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "0c6c1c795e9e8f30e68a6d4fec738fae8c7773fef4fddd8ace2e633cff76eea1",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "4d6d6ec7bd21f0b973c7b20426e509849d22d33a7a6cf995f3ddf4426bc4d904",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "0099715b3ddbc29451138c4304c744757f1bd4288dd57fa9b1900a5fab6a667f",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "fd342ce3e99d9df6de8fcdf09ff9735887d7025d88ba9814b4c73cff24691b26",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "993e6d85270d01ffba87c3a65fa6e500ed0edefc169cf7e19ab5edce3b40939c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "5bbb74136597f560ef6584173fe535acde328b3837aec995801310e77fcf5b48",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "6394f40dbc799909ee239e42c25d08b5b2af0ad0c8aa30f37553e936f1c1dc4e",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "43d69d5b3c57530fa06537afc946aa78cb84e8f0e29201c33136762fb4a1d7ea",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "b4cc4f08df6a6701036a24150236a2012688d81f11a635dd372dbedcedae919f",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "233c6cecef5e826bd1aea7c7c603fb86fc78299d2016c4d3afcb0c1509eff001",
    )
