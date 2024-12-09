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
        sha256 = "577509870c439086e0dab6699ead1c7581ee36df7a065f27cce8f80b0531d1da",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "80be74a19d7e2e7b5c2084cd357d9fec30fabcf037b2eb68f056899940c06a27",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "20067779045521df4314ef3799ee38cc992ffdafeef5bf5ad94287b23e613784",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "67670f37c778010e63cf8c7d1b1b65d4fa48a1431e60186d02add30006080d82",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "208934197ebdd88f156689ac02b57aaf9847b1962c1f3753276025b4cb9cb53c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "f3dfd5e65482745911abfc40a99cafdc473d669932cedc17e6a48e65869a32d1",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "6ef8c358daabfe51dd6444f59c48117fe471c7cd0a1f7f26c70534073cfbef17",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "88f0a5d3798e47970213a240a75058a1529bca4b0939377a5115d129c5d646dc",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "75a7e403b76dfe99d78e84d4ec21a8c35112f12cedb094d10c96e390a8300819",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "290be4c5901f5b791bffe7edde1191b999504bc8eda3c8bb4aa70c271bd0e892",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "081ad14f043be4086bdfc2cc3f66b23707c0fe96e53e40cde5d87c38ff773d17",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "dc452f8206a2304b1d00127580f7321d708c0b3e6ee54bd8c4488b0ee040f19f",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "adf574cff4a5a172f66be4f69c42bfae8d9a816fe2f5ba8db40f322b077e0cf0",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "49f0cad7e77b3ea25a5295f120af9652c4189e208e03b537095ad535465dddc1",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "9ffb336f91b14de34fafc2dbdb5980fc2f81c2ebda863f5325ef8d784276addd",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "15b0b620029b8a9966ea0d1890a7b3a5f47ac98816b29e2b54031e0de376b8ab",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "abd5e443d4e02adbc794f5410afdce01de8ca0c59ff0b52fc60245a361a2ecf6",
    )
