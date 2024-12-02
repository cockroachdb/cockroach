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
        sha256 = "c204ebb75b183c6c2bdf1ead4c9252fc28efa9b03982123a44c1e314765df9ae",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "e21d757e2477559531a69d977d30676046c99d605f35582f574257261597a94b",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "311452eead0b235f0671ca9e015146f855fdb254ce9094e2cf03ac5c907da88a",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "61c8f6d3404d43dda9bf29ef808ce95e4affa36351c8825f3122386e0091771c",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "885705972c0d6f44a56bad34d24ed09620155bdfad9b0438bcf3ebbe10cefebd",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "fce90a84a24993094e187cb4afa3d479c134abdaa4df2c8402eb0ba36fadd020",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "b70f3d43b959d918df3894f962e0f7fbeb2d03d6ee3966f37f1fe3eeebe23d85",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "5acf53f9586d0ebce6a298dde44f2c787ec8c71e40d5b6e7d2f20aafe25c8172",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "01e6f3908e3aa24e45e7807df6b8935abf0398741b22ec6522cc8d171bc026d5",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "e863d5ea49f811de0321c4a30adb716fb79e2989c76e64ea2b0d7e8615958bcd",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "46ab83965759dd9b8257a38c2d7e04615c312f01c011b3f6f707a881d8288127",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "bcd8f4256bbd43842f7580ad9c5689a10c7b843bddbfe897f7a8ec59bc0a1adc",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "085e956f4ba40a6975fa18464f527cbb273de148633193bc0a4c90d94c8d99e2",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "b6576585eeb285b2e0e14d740b234e7901b468d440251e2c1f6106f8dee4d1e9",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "dbd823e8bbdc8a673ec4b0ce561e411468c07ea585115a2594df4c2c4a079ba5",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "403cace092320a28fd32b0ce791fb1f5168cadbd2238e9a91e98899c2f8f9f0f",
    )
