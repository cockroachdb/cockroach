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
        sha256 = "bae849e42c2788582bc50d51c48e9bba682dcbe362d78e43bf2c2a750ddca1c5",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "1e401ae0fcd6429cf45f5e775966bbd7659154a9298d48d7430cf639546bbee9",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "70bf091c30cdf7068515e5d6fd2a8dd94dbdd0adaf487fc620682ecbf8b42432",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "ddadeb1d1c049b645a1bb60521f0fb7566190c05a62bacc8fddf0bb489520d41",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "9a86bbf0413a2a59bdd42b68988ad689d0b41988ab68b6190dc76995fe3d5abf",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "53529aeb57c86c105e47c7087293bed6015b717885c655b42f59f3d41caba368",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "2279a0307ede45317f54b8715bbcc5346f8b9e0a1f155f2258716f632480999f",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "e6c20447cc0c1fac808c65a33b3a68f7f40ea86e9702e7b22a9615af9bc46761",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "f1fc72f198cd708c2fdf65a34edd33a10774a284522f91767edb008b9f9888ea",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "3292150c89d16c620332f109f97234f3144340b2eaf4275302ddd7d4937be323",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "899876960787afbb1bd10d0611180bf47a09fb17691ee1a2dec76e5ad526d18f",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "b4c23300c850967577706f21e6e521ccf3d4b638e3dd1c1c86f1c894f0dd2c41",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "07511f677ce38332e510fa96fbe949126b22bb9f57c1864dd82605e80c3c30b2",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "df02d74f66290a1479ed05da1e448694b0de6bab9a1a7b72c8299f30444ad97b",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "4e7ae6ef4bb2f9a4affde29877fbf20176be2f859fc7abac6b568ed38953cf1b",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "811a41a5bc778ddaa518e24e2f738fff2d96eaae62239c0a2d15510c273dcded",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "40fc37cb8c66a2ca9220107b57f0339199adad2a23849f70e9b4a6d2495b8db1",
    )
