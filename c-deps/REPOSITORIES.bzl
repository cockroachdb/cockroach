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
        sha256 = "1c6592aa8507bd83a7252143578bc84e9731d09ac79f7dda69b1476ce11653d6",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "d84c705dc75d7b5287a62d10f9c54fb6a8e36920ec421d03fe5711e23af43c15",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "ff6fd6cd17600c3fb4571edf0400d27d7d606c25ba0ed04c9459dbeafbab07a2",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "94bef24e6d7b2e82282a83f4fc33fe47e455e8ea75f3509c45790bf082321e5a",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "0e09b30a9ca8872fce51ed115d27cb29c1e3586f433c2e805b396358e9e86adc",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "68996e395f5520026f753d2485cecf1fe67f190f11bc9436852afbf7e2702eef",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "eb5e5be91b772d3324be722d6a12d033f9194c66b7dd46b99c0ec4684ab02062",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "1053bef2dfaee4a3a9da9cd8c8a821603435ca8d228fe45d68b98e0ea12b5bd9",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "f7505ee5db337b8fe722bb9bf00dbf53ff2f11eb4e1929ac3bec75ecedc0f783",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "a87b5e57be46c7e8fad37417d880edfe8616ac44613d4921e1a8d72913713ae4",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "5543ef1db69587d96187ce1ab4e5ea1e6966effa9f8c8c5f577b96b1bba5bf87",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "bd8bb1e92279abacb2bb7c8c76eb21c85c1f41ddc28e559e9ac78fc08d52716c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "8eec1e0ebe5d64a8b28da8b880b106201c66659c322f958a3e9394f5dced77f8",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "9919729ba645a327cbe2c2b5a9c7c4baeeff45a65ae810045ea5bc7b678e352e",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "371e8f91d10b6a12c7d22f91b0e612f61a405f90c86d7affd05ed56d748089a6",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "5ddec132e1f075eb3664630eeb5ac208588de01a56a8ac70e3a7cf4762ab55c1",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "a4a9c582c89b3f30a0e41f755c8ef0f374bec345856243b08d676737486a28c9",
    )
