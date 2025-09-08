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
        sha256 = "1d5fa0bfebd7cb6a25b5e66fbdc4669f5bf3c9a30ac6b4696b58eba0af248929",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "ba401e0ef96b52caf9ad9f3d055806b3e3b3673f6daa8102526fa343bff9a546",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "abf87d8843d43ef47e8c8152944cb73b68b820d396e127b62aa3d3b30ba19be5",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "3ce6c4faabbf9bfbf795589e4e3b44cca1e4555e25332cbd3a9abe944ba388c9",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "4aaa1e8164fe1d1617286b663a5e00b5217bc022224048a55dcefde097d15425",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "2a205848a25126c2b66aace211eec2a868867ea70c949e57ab50fb3afe815a88",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "d9ef1dbf1effe9c950976ead65ef469978c89905fe199984340581506a4fa4b8",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "af3ece1c01f0a50eb2750478b0da2bbce8841f8f163929797fc5c784e7453f76",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "86b107f08f280bbcb7c95996cbd7ec75e0fc8323bd091643fd4d230c38f35ac1",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "cd96b1721d088b0a27746204e0ba072280be025a80a0ce550751e19ad6797f06",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "8d28434cd175f0a32dfdd8ba8a5fa44c3d04d1e53cccfe9dbb3c6e301a03a47c",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "7959c82bd4f3eab11a6163b05856b62a33bc5a0c69c2df62e181ff38f08c89c8",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "4a324242de5f4b2fb90994705c663f1b886352058416c028d1ffde67a8507c05",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "a4b0bbb056bb462682b49ec34816f02c71047b38733d50d8de78b737c892db61",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "0837fd5721deb6aa1dcaeefda316db6a96dd85b4c5a142eb3f0ad48e76eb974e",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "a61f4faf7a7d017a194c64b453a38c982423ef3678fa049dbf114920759da59c",
    )
