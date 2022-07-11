load("@bazel_skylib//lib:paths.bzl", "paths")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain")

# NB: URL_TMPL and LOC are used by generate-distdir. Don't change the format or
# name of these definitions unless you update generate-distdir accordingly.
LOC = "20220708-170245"
URL_TMPL = "https://storage.googleapis.com/public-bazel-artifacts/c-deps/{loc}/{lib}_foreign.{config}.{loc}.tar.gz"

# NB: When we link with the krb5 libraries, we want the linker to see them in
# this exact order. To that end we pass this list verbatim to the configure_make
# rule for libkrb5_foreign and we use this in _sort_static_libraries_key below.
LIBKRB5_LIBS = [
    "libgssapi_krb5.a",
    "libkrb5.a",
    "libkrb5support.a",
    "libk5crypto.a",
    "libcom_err.a",
]

# For libraries in the list of LIBKRB5_LIBS, the key we use for sorting is the
# index into that list (to ensure the list remains sorted). Otherwise the key is
# just the basename for the library.
def _sort_static_libraries_key(f):
    b = paths.basename(f.path)
    if b in LIBKRB5_LIBS:
        return LIBKRB5_LIBS.index(b)
    return b

def _is_static_lib(file):
    return file.path.endswith(".a") or file.path.endswith(".lib")

def _archived_cdep_impl(ctx):
    # The includes are in a folder called "include".
    system_includes = []
    for header in ctx.attr.headers.files.to_list():
        p = header.path
        for component in header.path.split('/'):
            if paths.basename(p) == "include":
                break
            p = paths.dirname(p)
        if p not in system_includes:
            system_includes.append(p)
    compilation_context = cc_common.create_compilation_context(
        headers = ctx.attr.headers.files,
        system_includes = depset(system_includes)
    )
    static_lib_files = [
        file for file in ctx.attr.libs.files.to_list() if _is_static_lib(file)
    ]
    static_libraries_to_link = [cc_common.create_library_to_link(
        actions = ctx.actions,
        static_library = file,
    ) for file in sorted(static_lib_files, key=_sort_static_libraries_key)]
    cc_toolchain = find_cc_toolchain(ctx)
    dynamic_libraries_to_link = [cc_common.create_library_to_link(
        actions = ctx.actions,
        dynamic_library = file,
        cc_toolchain = cc_toolchain,
        feature_configuration = cc_common.configure_features(ctx = ctx, cc_toolchain = cc_toolchain)
    ) for file in ctx.attr.libs.files.to_list() if not _is_static_lib(file)]
    linker_inputs = [cc_common.create_linker_input(
        owner = ctx.label,
        libraries = depset(direct = static_libraries_to_link + dynamic_libraries_to_link),
    )]
    linking_context = cc_common.create_linking_context(
        linker_inputs = depset(direct = linker_inputs),
    )
    return [
        CcInfo(
            compilation_context = compilation_context,
            linking_context = linking_context,
        ),
        DefaultInfo(
            files = ctx.attr.libs.files,
        ),
    ]

# _archived_cdep is a rule that returns a CcInfo for a pre-built cdep in cloud
# storage. This shouldn't be used directly; instead use archived_cdeps().
_archived_cdep = rule(
    implementation = _archived_cdep_impl,
    attrs = {
        "headers": attr.label(providers = ["files"]),
        "libs": attr.label(providers = ["files"]),
    },
    fragments = ["cpp"],
    host_fragments = ["cpp"],
    toolchains = [
        "@bazel_tools//tools/cpp:toolchain_type",
    ],
)

def archived_cdeps():
    for lib in ["libgeos", "libjemalloc", "libproj"]:
        for config in ["linux", "linuxarm", "macos", "macosarm", "windows"]:
            _archived_cdep(
                name = "archived_cdep_{}_{}".format(lib, config),
                headers = "@archived_cdep_{}_{}//:headers".format(lib, config),
                libs = "@archived_cdep_{}_{}//:libs".format(lib, config),
            )
    for config in ["linux", "linuxarm"]:
        _archived_cdep(
            name = "archived_cdep_libkrb5_{}".format(config),
            headers = "@archived_cdep_libkrb5_{}//:headers".format(config),
            libs = "@archived_cdep_libkrb5_{}//:libs".format(config),
        )

def archived_cdep_repository(lib, config, sha256):
    http_archive(
        name = "archived_cdep_{}_{}".format(lib, config),
        build_file_content = """
filegroup(
    name = "headers",
    srcs = glob(["include/**/*.h"]),
    visibility = ["//visibility:public"],
)
filegroup(
    name = "libs",
    srcs = glob(["{}/**"]),
    visibility = ["//visibility:public"],
)
""".format("bin" if (config == "windows" and lib == "libgeos") else "lib"),
        url = URL_TMPL.format(
            config=config,
            lib=lib,
            loc=LOC,
        ),
        sha256 = sha256,
    )

# cdep_alias defines the build target for the given library.
# If force_build_cdeps is set or if we're targeting an unsupported OS, the alias
# will fall back to :{lib}_foreign. Otherwise it will point to the appropriate
# :archived_cdep_{lib}_{config} target as defined by the archived_cdeps() macro.
def cdep_alias(lib):
    actual = {
        "//build/toolchains:force_build_cdeps": ":{}_foreign".format(lib),
        "@io_bazel_rules_go//go/platform:linux_amd64": ":archived_cdep_{}_linux".format(lib),
        "@io_bazel_rules_go//go/platform:linux_arm64": ":archived_cdep_{}_linuxarm".format(lib),
        "//conditions:default": ":{}_foreign".format(lib),
    }
    if lib != "libkrb5":
        actual["@io_bazel_rules_go//go/platform:darwin_amd64"] = ":archived_cdep_{}_macos".format(lib)
        actual["@io_bazel_rules_go//go/platform:darwin_arm64"] = ":archived_cdep_{}_macosarm".format(lib)
        actual["@io_bazel_rules_go//go/platform:windows"] = ":archived_cdep_{}_windows".format(lib)
    native.alias(
        name = lib,
        actual = select(actual),
        visibility = ["//visibility:public"],
    )
