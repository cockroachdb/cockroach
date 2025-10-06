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
        sha256 = "3c5ffe12ea3e1b92f80f98e509c206b66a780b175c9aba2b085f1c39377c982f",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "282967fa0b7ab2d134da7485239ea0e816b4c2b0ca2b59a7fcd30f6d7d0fce3c",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "5e392cd33a5c16ed8f618a21f311cee0518fbfa2fbc7800c819cb0d0aacf833b",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "94f62ceaad3ca235ff932b2256b314a21a62774efc3e0d898c05f5d1d74b7578",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "820b804268e8f69a9f5592d03a5834bb202939184c623f6aca8a00863c4755fd",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "2eb8217364c5592ea8061ee1aef7406c034cc43a543fb7871fd28c59b5765a5c",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "6796b65289835622ee1b634a47cc784c5b764ca63959d77ccc54c66c5afd708d",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "06eedc730402719f444349935ca3db928e510cfd95f3aa32eafbc3fdcc10c5b2",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "ad84331c25abae7bec02e54556e38804d5eab1f14c542a369660b040ca25ee14",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "180dbd0156b89079fedacc9f2d8f0fb3082616ee21ffc1a3f6d1a99a67dfc56d",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "4b4dadf30e225693723612ede7fc5138eb1ad1b863db744c52099535dbdc3c00",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "9ec86dcf655cb441a3dbed29897b27b2fbc37830816d07f1da58e197c332a3bb",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "9a3fae2482dd837842e755a10b70d7a15b3ff9f431b81e63efc2f20a1885d174",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "3e3220bd83009de29185772be26022ae219cb006eae1d8dba87292206ce9f4ea",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "4197f25032d45b6e8acb6446914f6153c0fc48e0d75d2eba99d58806622fcc98",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "8284b57f832ab3c5353860ad715e8844c93bf6822b01cb5108b5b494ea90a2dc",
    )
