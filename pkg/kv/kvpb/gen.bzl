load("@io_bazel_rules_go//go:def.bzl", "GoSource")

def _batch_gen_impl(ctx):
    srcs = [src for src in ctx.attr.src[GoSource].srcs]
    ctx.actions.run(
        outputs = [ctx.outputs.out],
        inputs = srcs,
        executable = ctx.executable._tool,
        arguments = ["--filename", ctx.outputs.out.path] + [src.path for src in srcs],
    )
    return [DefaultInfo(files = depset([ctx.outputs.out])),]

batch_gen = rule(
   implementation = _batch_gen_impl,
   attrs = {
       "out": attr.output(mandatory = True),
       "src": attr.label(providers = [GoSource]),
       "_tool": attr.label(default = "//pkg/kv/kvpb/gen", executable = True, cfg = "exec"),
   },
)
