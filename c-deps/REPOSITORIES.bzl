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
        sha256 = "ba5abd50a78c37460b3bf71e511c27c9ccee47b62f39fb95e2f0484ca81e7483",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "fc49f5f31603c20743eb6278497777d18681c7ad51a0ed32fe246e7d1c55f12e",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "0ab2d7116f5c52c7f035e492b8899f9e632450f4c0c1db81b9da45bb40317950",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "bfb537ee11fb1442d77e26d273c42a06366b2687e1936cca166f4fd25ccd42dc",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "0817ec0f0b6487c8204e7456025ad9bf2aad644e67842d2089c79e8eed0e64ea",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "b6c969eb34369e5964f3d8d41efe4d0d19a1dd298ed8f217cfbaa2718e77dc9b",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "624631c7c8e79ec97074ab7bf2c19102c82a2c897fa3465a97c21809a3c35579",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "e9fce505246165c34c4cb00b67076cae2319c5203db11a378a76c17cea204722",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "10bcfd316c047afe00bb6d5ad519cbd2b4c8b9d35ca197fed771f6d3fbbb7b66",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "9f786e0e8200ce8f55f0328c283ddbbb77a5fcd03437319335e0fddf9fb27a66",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "7706dcc20f083f621ba891eb82864e980ae352f34add33753100085d12ab91ec",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "1db3ec640b3992a2dd693df9bf5c52c55b43f387399ca885f1692fbbfa6baf88",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "06f046643bbcbf071108c4dc21f5bc9b843f597871d88f95760eda63d4ade822",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "1ab617eb51b017d27d71ba08a065b6a08ef5410e9aa5554669196b7cf28214c1",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "windows",
        sha256 = "221408632feebb02f0793900aea1ed477fbb51cf8b278c4d6eea61f38870ffb4",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "1f5ca83705c961b89745fc4f80b8363995f973800be210a5a5fd423d6bd17a22",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "b819b17740b2a3418d62d2f6db8b245094458180e1e5e301e9f0f4257696fef5",
    )
