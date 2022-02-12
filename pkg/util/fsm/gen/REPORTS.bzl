load("@io_bazel_rules_go//go:def.bzl", "GoArchive", "go_binary", "go_library")

def _gen_template_impl(ctx):
    archive = ctx.attr.dep[GoArchive]
    importpath = archive.data.importpath
    basepkg = importpath.split("/")[-1]
    ctx.actions.expand_template(
        template = ctx.file._template,
        output = ctx.outputs.write_reports,
        substitutions = {
            "{tmpl-full-pkg}": importpath,
            "{tmpl-base-pkg}": basepkg,
            "{tmpl-type}": ctx.attr.transitions_variable,
            "{tmpl-start-state}": ctx.attr.starting_state_name,
        },
    )

_gen_template = rule(
    _gen_template_impl,
    attrs = {
        "dep": attr.label(mandatory = True, providers = [GoArchive]),
        "transitions_variable": attr.string(mandatory = True),
        "starting_state_name": attr.string(mandatory = True),
        "_template": attr.label(
            default = Label("@cockroach//pkg/util/fsm/gen:write_reports.go.tmpl"),
            allow_single_file = True,
        ),
    },
    outputs = {"write_reports": "%{name}_write_reports.go"},
)

def gen_reports(name, dep, transitions_variable, starting_state_name):
    template_name = "gen-reports-" + transitions_variable + "_" + starting_state_name
    _gen_template(
        name = template_name,
        dep = dep,
        transitions_variable = transitions_variable,
        starting_state_name = starting_state_name,
    )
    go_library(
        name = template_name + "_lib",
        srcs = [":" + template_name],
        importpath = "github.com/cockroachdb/cockroach/pkg/util/fsm/gen",
        visibility = ["//visibility:private"],
        deps = [dep],
    )
    go_binary(
        name = template_name + "_bin",
        embed = [":" + template_name + "_lib"],
        visibility = ["//visibility:private"],
    )
    lower = transitions_variable.lower()
    native.genrule(
        name = name,
        cmd = """\
export COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true
$(location :{template_name}_bin) $(location {lower}_diagram.gv) $(location {lower}_report.txt) 'bazel build {name}'
""".format(
            template_name = template_name,
            lower = lower,
            name = name,
        ),
        outs = [
            lower + "_diagram.gv",
            lower + "_report.txt",
        ],
        exec_tools = [":" + template_name + "_bin"],
        visibility = [":__pkg__", "//pkg/gen:__pkg__"],
    )
