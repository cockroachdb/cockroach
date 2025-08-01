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
        sha256 = "ae787cc08fdf6b511859c5c67e5b834a9f6cdb57aa49d32f9226a984bbeb2af5",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "21059f2c065b35fa7cd39a19f882e03dbab24480d0eb787d03dd458bbba92b4a",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "06a6c93b677e79169ccf7532beabdeb863d0f57bb3db41796a75dbe47ff3bceb",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "6347a75c1e96e37466ad9442aeafc4f3df8aa56a1afcd006935735d4b62ef3c1",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "edaac8486c7da5a87c3857e52223a35e3533286786c0109847de1fd142884c2a",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "745930cc073b09f5a25690e7b3a273a96dd6943402ba425ad916c9555a2a016c",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "57dcd4f9d7ffdd5a03127aa7aa2c11a061b7ab457f0c4f717c65914c1aa8cb7d",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "129b381500e41126e0fcd0d169df8ec0625e2e6df13d5d10ee81f45312f6af74",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "2b762d57dd877e54fee402afbdebe433bbd4ee636accf782ec44633c6f1e380b",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "255632b4fddebd7cb4799f255251a8137256e207a402900ca7de7be496a1a238",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "b0a672f24748c24c941884c04413fd8e20afd6bca95e0451f3cfb15dd2cc1eb9",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "401252bba664cbbd9d9329fc068b04b0cc01e390e7c7f419a6cba081c6e950c2",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "0a73f3f88183ce7aacb8a105077b32ff6ee77216adc776683cf64f75eb9a6513",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "a35706bf0112e9652d6e41501113ee0f0f392b67e495979c867f71f17cf6ce7e",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "c13afc513257839762e423af9a52c3c9248657142ef25db0aad63e1a1a2a4ec9",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "a33b64a784e856dbb3c84a52075227c48f0ff52a858639c3737a8911201d0c65",
    )
