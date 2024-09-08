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
        sha256 = "23f80b64a2699929e90e0842e9bd4921caef84caf1de30922e76b8a8747acf7c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "0c70d905856015438c021e997aed14e16ed3bba48f0f878583ea8e3938437e80",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "3aead8da993877f39e9be44e7b5cab7dd3134aea7f616d9c88b3fea11f6a4c8e",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "6de66b050784c45dcbcd934e3a4b4d1bc6c9eb9adbfb0a8f9873615593858601",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "b4cf94bfe7441ee99a035f47cf99f8de9f067d3ae41f3c03aa654320162cdd85",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "ba5f76bcd0650f59f4829ef32c2b9dcae033b469eb0ffdcd44a8d1bad10fe420",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "e4bcf141ec06e10ec6c9267ee8263922991b7f9ce0b6c7a854683bc30233c8fc",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "c134475f9b98eabb1d9849161710cbb88e41053cbe6297818105b88190b371d5",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "badb26eecf059304197a79e9e1af20ca375b5062d838dccc30003f402316acb0",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "7883064d61e996198c9375d185022fa9d46e48ea1f634683773a8cdd1664860f",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "21c9aa75b7f0f6f94bb24bf5fadc4bd3c3e6e7a944db91ca2941b4bf40a6bacd",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "203d371a21f2982bee2863da665b687916a9a23893e29cc467722379fa4c2947",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "5f4524c5f97e9f88b553dafc2a320f0c30784af0678b0b3f732662495227be4d",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "ac909be9739baac1aafcaa7fdf12837dc0c284a8c31c8362d38d03b318b86aea",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "54d47c2bd65127a0f485d0a10821617233f37363b58fa3d24b2a08cc4973889d",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "0eb7874843712734708b2dbe002329a6d0d7370fde9df13b972472fd6f867f82",
    )
