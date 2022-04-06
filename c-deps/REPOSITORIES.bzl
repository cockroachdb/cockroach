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
        sha256 = "a9ce2803197243bf3d21557667b4a3b76f08befbb161e32ad2bbabd74029fd60",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "e849bd4a7abb1d00270eb1105655be833a6cd9ae8c8b389de32f9b75098bc6e2",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "db0facc2c38f732cf965fb57c6c236a7316aca576dbeec92504d20d95a88302a",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "86bca5abd7c8cc35bb442e86441dacdcf53a13a3d637df6c5182425afff84130",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "eb13317c26d323a366be0be54235364e36fe7fb83628b3b254f53cefecfe1f7c",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "00f7c6e7481a6240a284566824b01881caa555a9761fa9d0e0c4123af634fd3d",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "544ee020c050646b4015248e1baaf815d333e394534edf89ac41c0857be9fac1",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "4d8c682aadad5115fa31221c25581fcc0ada51ef20f64e85d1eb0c48a7116b22",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "40119c33bb1bf0bc0c685c1468b9cf720ea71517eb99932907626012bbedfc7d",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "16e7ca370de9b7244914259e2810734664b4466167c31c7c5107d16eaaa17d2c",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "997e998fb8d19379b7642305a4604bf826217f0381032bb079fcdb1ff8c00708",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "e35abe052c0cc15db6328dd7dfad1882a24415d226bd6fe3f26955eee9add11f",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "6f546e8f2d154f561735aa527c9ee4f12740f8afbd5d5ef87812230eb6bae2ce",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "9efe655a8751212c76e34e573fdc139e1aa919c3acb06759b089af3472a43571",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "43ea718397688416db884c99880a7785b50156c3d9cbd0176198d219a7e2a25e",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "24620658fd5e8b8b767433166af9b6da0ed6afd1ba6e774db5138c1a327c535a",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "026c2d888224d54c1a6e13ccaa30f809787eabe3a8b298b51aeb61d712fcca80",
    )
