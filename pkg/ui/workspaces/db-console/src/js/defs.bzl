"implementation is borrowed from https://github.com/bazelbuild/rules_nodejs/blob/stable/examples/protobufjs/defs.bzl"
"it is extended to consume multiple protobuf_library targets instead of single instance as in original example"

load("@build_bazel_rules_nodejs//:index.bzl", "js_library")

# TODO switch to protobufjs-cli when its published
# https://github.com/protobufjs/protobuf.js/commit/da34f43ccd51ad97017e139f137521782f5ef119
load("@npm_protos//protobufjs:index.bzl", "pbjs", "pbts")
load("@rules_proto//proto:defs.bzl", "ProtoInfo")

# protobuf.js relies on these packages, but does not list them as dependencies
# in its package.json.
# Instead they are listed under "cliDependencies"
# (see https://unpkg.com/protobufjs@6.10.2/package.json)
# When run, the CLI attempts to run `npm install` at runtime to get them.
# This fails under Bazel as it tries to access the npm cache outside of the sandbox.
# Per Bazel semantics, all dependencies should be pre-declared.
# Note, you'll also need to install all of these in your package.json!
# (This should be fixed when we switch to protobufjs-cli)
_PROTOBUFJS_CLI_DEPS = ["@npm_protos//%s" % s for s in [
    "chalk",
    "escodegen",
    "espree",
    "estraverse",
    "glob",
    "jsdoc",
    "minimist",
    "semver",
    "tmp",
    "uglify-js",
]]

def _proto_sources_impl(ctx):
    return DefaultInfo(files = depset(
        transitive = [p[ProtoInfo].transitive_sources for p in ctx.attr.protos],
    ))

_proto_sources = rule(
    doc = """Provider Adapter from ProtoInfo to DefaultInfo.
        Extracts the transitive_sources from the ProtoInfo provided by the proto attr.
        This allows a macro to access the complete set of .proto files needed during compilation.
        """,
    implementation = _proto_sources_impl,
    attrs = {"protos": attr.label_list(providers = [ProtoInfo])},
)

def protobufjs_library(name, out_name, protos, **kwargs):
    """Minimal wrapper macro around pbjs/pbts tooling
    Args:
        name: name of generated js_library target, also used to name the .js/.d.ts outputs
        protos: labels list of targets to generate for
        **kwargs: passed through to the js_library
    """

    js_out = out_name + ".js"
    ts_out = js_out.replace(".js", ".d.ts")

    # Generate some target names, based on the provided name
    # (so that they are unique if the macro is called several times in one package)
    proto_target = "_%s_protos" % name
    js_target = "_%s_pbjs" % name
    ts_target = "_%s_pbts" % name

    # grab the transitive .proto files needed to compile the given one
    _proto_sources(
        name = proto_target,
        protos = protos,
    )

    # Transform .proto files to a single _pb.js file named after the macro
    pbjs(
        name = js_target,
        data = [proto_target] + _PROTOBUFJS_CLI_DEPS,
        # Arguments documented at
        # https://github.com/protobufjs/protobuf.js/tree/6.8.8#pbjs-for-javascript
        args = [
            "--target=static-module",
            "--wrap=es6",
            "--strict-long",  # Force usage of Long type with int64 fields
            "--keep-case",
            "--no-create",
            "--no-convert",
            "--no-verify",
            "--no-delimited",
            "--out=$@",
            "$(execpaths %s)" % proto_target,
        ],
        outs = [js_out],
    )

    # Transform the _pb.js file to a .d.ts file with TypeScript types
    pbts(
        name = ts_target,
        data = [js_target] + _PROTOBUFJS_CLI_DEPS,
        # Arguments documented at
        # https://github.com/protobufjs/protobuf.js/tree/6.8.8#pbts-for-typescript
        args = [
            "--out=$@",
            "$(execpath %s)" % js_target,
        ],
        outs = [ts_out],
    )

    # Expose the results as js_library which provides DeclarationInfo for interop with other rules
    js_library(
        name = name,
        srcs = [
            js_target,
            ts_target,
        ],
        **kwargs
    )
