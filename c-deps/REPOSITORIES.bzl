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
        sha256 = "755a734ed4f44328377f04ffe7e2a53f6552aee6de5f448ae34ade5bdf0047b3",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "d6e222cf1f09834216af546775b23a8a13bdb5b49cfe45741e1aff5c2a739ee9",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "f1fe740b931f29289fd9cad7f877705072678c83bd5c9ef899cdbf3373985d2e",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "4bcd7ed82d8487370cad34f68096e160e768630f579fc2e9d09c72b0ebf3f741",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "10e55138113fb43ad4d4c4a38ae77b34d9d74b0d54c46f0bfa9ae6cf6551db46",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "10e34f6c314558b2ae092a55685eecad0767b161c7aa14e7fd5b87d9e7b450a1",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "6fe0ddee54c71f5afd71effb728a1227beb43ca6977205e56ee9eaf5b6f15422",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "dbbcf9a633d251530ad7b698934b6752eca347f34985060700d3ec187b1f3498",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "bcf05b88b102628ec878a91086d0039fd9460d17b6431e1322310b3820103f23",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "44dd0f9b296f5885f0bf5936ebbe0d6f574b4c067330d66b1f55b7fd15e33066",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "aa618a6525c0df669ce1231c837dc46a1a789d54231ae3dcfcada599d0845b22",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "bba59eb60c31c34bde10ac4a34d2210d201dc9d8e188be0aeb2d3038d148f63c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "e444612cba97d31ae8ce23534e2e0416c2ebf79bb505e38882cec5d8dd7b60dd",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "e3206fa7c2544d1817103c3c61f132d342b5d4a07404d1de4582da968b622749",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "27aea836a821920c8b444f9ec251c32af3842d6a2a084f286632e819b8c03d71",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "3db4fe746a7591a87b94bbbfb35d727a51506775ea36b9f634eb770b486e9c7b",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "4d935601d1b989ff020c66011355a0e2985abacf36f9d952f7f1ce34d54684ad",
    )
