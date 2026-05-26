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
        sha256 = "b8e21e54d5a5e801eaacff05e5e7c2e3edda4b696524155b6fefbb83cc055ebf",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "26657cf71b3eaa9ed43b16d154f397ab08262fcf4dceb2fade20dae24bb10073",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "aba12de949c4e4df4114f4e293de91dca37e797cbfb3c59b4e7fcd0a556cb9cf",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "0171e5bfef789e444cc80596075f6f0af770290ba7dd0ac10e49b385033f34a0",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "7eedf21fd74e357abef59c54fadd3406f8f10abeec86c08f7e43571fd5354ddd",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "c7bd97cdab009b7abe74a8ed6949f0622d62bc48c80318034b0396a845b77ece",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "66d7c01965858cd0a6f6d3fc153e666667213c3d593c58af34987842f467f4b8",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "8dc62ffbfda854c0844fd78271236974f970fe77b46dd1897965a5edc0c35451",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "fdc9ff844b8cda3b56ec1c2aadb8bb67de21d78779d3ef170d7f9f1e0390e0c6",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "c4440cbf985c6b5b4d2f7cae991cd2f045f557f6a2aafca905f997bf8b319d5b",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "ae52bcb64da687b18b3dc0d2f77a229212b7527b5e7212265e23f61b09840855",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "9bbff5d6db89c5c675d98ce98931faf23eac265019bec45b54cd2e95682a44fe",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "a40e70e6629890e513bed9448d8e702d76eb5b7ef067e11a0c5ec7522b728868",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "cf49f21c7ca8b7e4b9a13c6d9b9ccc6f94e5706255bdf0e95a4f83145dc9a11f",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "2494aeeb85b75b051ab86df51efe69ae5b75eb2e1aee40e482aed7a66ae19270",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "ab39cdf9bf6f584104bad860071bea240b9d6c67dffcde9cff1eddd078dec79e",
    )
