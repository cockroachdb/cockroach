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
        sha256 = "f737612dc1a1b28b0b6d32045223da059ad3c5c0be05ed9a3577719d56bc05d8",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "c54ac181966a9003a82c4aea3b36c64b960f349665bf14c195012a5de8c3962f",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "84526befa7fe16454d9b56b9b6a0c4e76064fdd4cce0571f2d17be146f8a12c3",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "0bab9dc5df6211b1c5efb0436a0f48afc4604196dfb88a05372e5d7615262004",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "f1584adaded1396e237b16c0ef7a386a2ba1aa7c6f8f3516adeacca49c78c304",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "b761375b5a24bc9375d791d93fbd0aba4393bc0c2ceafbf6cc3b6a3f7b8678d8",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "0d567cc9a0da9ec484afa22c16335130ca8fe284194ca43662e20ca45633f6c8",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "f21ca59c127eddbe3c15342cf274c6302c6ddd5a0dbf15655a4bddf71e1ec994",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "57db552807df20101a7a85ee895fd5c8c2cd7ccf9600e1122b69c36c2d2caa72",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "5de3ecfb6e390309597b5be7247acb3333273f1453866851dfd73af7c84d1e3f",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "7e629de098c063f17fc4100c075e5227927b003b24fc2c376066bb97d78f6c5b",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "2e30b7a16137b452ad00814f035d078ba9538459b80224591700ff56164f73ad",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "dc1e385cec8f116241e7860dd8622fd58391bd3301a01ed2887712c66715505c",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "15524720d53be5d1b024762250a560a4e32403d9a6bb29d1d519ccefe74c94b6",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "ae16c8ef3c910a7add068ab4af14d6829203a5ed3cd8d14b8afdcf2ddd0cda5c",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "901e26ac4353fbaeb541c5219c4e996474448f410d7d7d3111f238ce336d38cb",
    )
