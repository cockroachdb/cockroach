load("@bazel_skylib//lib:shell.bzl", "shell")

def _gen_script_impl(ctx):
    subs = {
        "@@PACKAGE@@": shell.quote(ctx.attr.test.label.package),
        "@@NAME@@": shell.quote(ctx.attr.test.label.name),
    }
    out_file = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.expand_template(
        template = ctx.file._template,
        output = out_file,
        substitutions = subs,
    )
    return [
        DefaultInfo(files = depset([out_file])),
    ]

_gen_script = rule(
    implementation = _gen_script_impl,
    attrs = {
        "test": attr.label(mandatory = True),
        "_template": attr.label(
            default = "@cockroach//build/bazelutil:lint.sh.in",
            allow_single_file = True,
        ),
    },
)

def lint_binary(name, test):
    script_name = name+".sh"
    _gen_script(test=test, name=script_name, testonly=1)
    native.sh_binary(
        name=name,
        srcs=[script_name],
        data = [
            test,
            "@go_sdk//:bin/go",
        ],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        testonly = 1,
    )
