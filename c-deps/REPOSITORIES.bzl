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
        sha256 = "7dceeeb85cb9c09c30dc667a89c6cb81bbc5fd4f748aa32e2a28860b1ea481be",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "b49b6d82fb07f888e910530156f679cf565a7680e169d3eebde239a1eb301845",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "01df3e15015d6c9833ed8e3c768a4d743a09d647ee78276f84b4887d2cee6ba2",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "12975a9824cbe84181e4992fc14e467c555c3fe53fc8408c7e300dbe966df6cd",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "3de84b39c79ac4a9ed3ff5003ac68518a451378f191c4f9e744d1250ea47fc00",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "574347682ce737dff07419e943b016d07f00215b2572ce9b9fc65461e887afe2",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "17fc28ae89e25c8246c94e96f7de0d2f0dfc119e0fe783f78e6fbcac578f3608",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "91a1490ee7ef9f90b4900b13dd735dc86f4759fba737593155c8efc720a67dc5",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "4f9c8a26e13999268f5a0205941cd9a515b53aef2373b502a8af085aa35d1889",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "9e5b33d9b230e8d4ae1f6b8f120081717a3f41d01c210694781fe34726543a97",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "48cee3a1c10a428022512afadc5fd2ba0b9d2e9809705352fcbcce7889b1d9ba",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "6d31a2b1b19037bd0c97a2a133967ce97ae559a0cc3437f14643f92e21e34baa",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "9f102e1d94272121c371037ab57f23140fcb26a74396564f86e9c44941e8e0ac",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "60ee8073733db22817426adb37dad05c8463b0d03bf1c5e83b9fa39fd2756f05",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "08964b7ff8cb147675e836a6efae3a989f77d22ca5400893d4316c875537e561",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "bc6e767c4e9e558fb3b54aabc7b5807058939f8b29c264fb8d80abcdfb39db27",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "561c09be157a75ff618f6c067d7afed55f0f6d56d493a0a930525e4e97e14cfa",
    )
