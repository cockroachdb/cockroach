def _flatten(*lists):
    return [x for l in lists for x in l]

def _sctest_gen_impl(ctx):
    ctx.actions.run(
        outputs = [ctx.outputs.out],
        inputs = ctx.files.test_data,
        executable = ctx.executable._tool,
        arguments = _flatten(
            ["--package", ctx.attr.package],
            _flatten(*[["--tests", t] for t in ctx.attr.tests]),
            ["--ccl"] if ctx.attr.ccl else [],
            ["--suffix", ctx.attr.suffix],
            ["--new-cluster-factory", ctx.attr.new_cluster_factory],
            ["--out", ctx.outputs.out.path],
            [f.path for f in ctx.files.test_data],
        ),
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]))]

sctest_gen = rule(
    implementation = _sctest_gen_impl,
    attrs = {
        "out": attr.output(mandatory = True),
        "tests": attr.string_list(allow_empty = False),
        "test_data": attr.label_list(allow_empty = False, allow_files = True),
        "ccl": attr.bool(),
        "suffix": attr.string(),
        "new_cluster_factory": attr.string(),
        "package": attr.string(),
        "_tool": attr.label(
            default = "//pkg/sql/schemachanger/sctest/sctestgen",
            executable = True,
            cfg = "exec",
        ),
    },
)
