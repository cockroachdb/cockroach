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
        sha256 = "7ad99432a0c09380604c3c55ce422649024818db92a9912d8e223e30001b15ac",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "7364c749452e4d62bc12b75669e971cbab3acacde791851b8e9aa1f80ab6a5f3",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "c80aef627ef24036fddd26c18600e4be594678c6b9935a930f8dba7fbdc9fce3",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "b00fcaea77c8e42a34fa18d0dda4211db01e9f27f402685c60265abd000d3ff5",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "80402249a2007bfbba8db82ba1bdc510d4ff12bf643f72fb8a9bcadf8274d9b1",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "b8872435726f170748acabb0c1b56c5dd65205a056c99b5c525db36cf7c64bd1",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "7350a695a316a8618db9323ab609ec5604123f2249e345193c625da8987d1e5e",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "96e72cc4871f9a5265deacef93243ad281ad1ef94805788a6696d6c978eba1db",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "66e25c31359736feee9481edd9f8c46446b4521fea0408c83c531067c572018a",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "7bb49812e427360826fad1c8727317d01c56383c2e973b67fbda04ee81b6c361",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "96771a33542beb72067afcafaeb790134014e56798fa4cbe291894c4ebf8b68d",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "564cd09a59cf6eeb83eb151d483679024195f780780ad546c3fb3a9c564d72b3",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "fc0abd2c6c4feb38cd17963c6b2594053e764ce23757958a25acb9db2c44d2db",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "b2c60ffe1f50c6e81ba906f773b95d3a6699538d57e71749579552f4211a1e3e",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "358ae921815ba1c795694bf3cf36f81125c0cb69f732ea05ebfb93204b3ddd8b",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "16de1e76ee8de4bd144dc57bfde05385d086943ca1b64cc246055c8b0cd71c65",
    )
