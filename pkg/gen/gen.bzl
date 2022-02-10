load("@io_bazel_rules_go//go:def.bzl", "GoSource")
load(":protobuf.bzl", "PROTOBUF_SRCS")
load(":gomock.bzl", "GOMOCK_SRCS")
load(":stringer.bzl", "STRINGER_SRCS")
load(":execgen.bzl", "EXECGEN_SRCS")
load(":optgen.bzl", "OPTGEN_SRCS")
load(":misc.bzl", "MISC_SRCS")
load(":docs.bzl", "DOCS_SRCS")

# TODO(ajwerner): Use the all variable combined with genquery to construct
# a test to show that all of the generated files in the repo are represented
# here. Of course, this will rely on actually representing all of the generated
# file here.
EXPLICIT_SRCS = PROTOBUF_SRCS + GOMOCK_SRCS + STRINGER_SRCS + EXECGEN_SRCS + OPTGEN_SRCS + DOCS_SRCS

GeneratedFileInfo = provider(
  "Info needed to hoist generated files",
  fields = {
    "generated_files": "dictionary from prefix to list of files",
    "cleanup_tasks": "list of bash commands to run"
  }
)

def _subshell_in_workspace_snippet(cmds=[]):
  return """\
# Use a subshell with () to avoid changing the directory in the main shell.
(
    cd "${{BUILD_WORKSPACE_DIRECTORY}}"
    {}
)
""".format("\n    ".join(cmds))

# Avoid searching the node_modules directory because it's full of
# irrelevant files.
_find_relevant = "find ./pkg -name node_modules -prune -o "

def _go_proto_srcs_impl(ctx):
  generated_files = {}
  for s in ctx.attr._srcs:
    srcs = s[GoSource]
    lbl = srcs.library.label
    imp = srcs.library.importpath
    imp = imp[:imp.find(lbl.package)]
    prefix = "{}/{}_/{}".format(lbl.package, lbl.name, imp)
    generated_files[prefix] = [f for f in srcs.srcs]

  return [
    GeneratedFileInfo(
      generated_files = generated_files,
      # Create a task to remove any existing protobuf files.
      cleanup_tasks = [
        _subshell_in_workspace_snippet([
          _find_relevant + " -type f -name {} -exec rm {{}} +".format(suffix)
        ]) for suffix in ["*.pb.go", "*.pb.gw.go"]
      ],
    )
  ]

_go_proto_srcs = rule(
  implementation = _go_proto_srcs_impl,
  attrs = {
   "_srcs": attr.label_list(providers = [GoSource], default=PROTOBUF_SRCS),
  },
)

def _gomock_srcs_impl(ctx):
  files = [f for di in ctx.attr._srcs for f in di[DefaultInfo].files.to_list()]
  return [GeneratedFileInfo(
    generated_files = {
      "": files,
    },
    cleanup_tasks = [
      _subshell_in_workspace_snippet([
        _find_relevant + "-type f -name '*.go' " +
          # Use this || true dance to avoid egrep failing
          # the whole script.
          "| { egrep '/mocks_generated(_test)?\\.go' || true ; }"+
          "| xargs rm ",
      ]),
    ]
  )]

_gomock_srcs = rule(
   implementation = _gomock_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(allow_files=True, default=GOMOCK_SRCS),
   },
)

def _execgen_srcs_impl(ctx):
  files = [f for di in ctx.attr._srcs for f in di[DefaultInfo].files.to_list()]
  return [GeneratedFileInfo(
    generated_files = {
      "": files,
    },
    cleanup_tasks = [
      _subshell_in_workspace_snippet([
        _find_relevant + "-type f -name '*.eg.go' -exec rm {} +",
      ]),
    ]
  )]

_execgen_srcs = rule(
   implementation = _execgen_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(allow_files=True, default=EXECGEN_SRCS),
   },
)

def _optgen_srcs_impl(ctx):
  files = [f for di in ctx.attr._srcs for f in di[DefaultInfo].files.to_list()]
  return [GeneratedFileInfo(
    generated_files = { "": files },
    cleanup_tasks = [
      _subshell_in_workspace_snippet([
        _find_relevant + "-type f -name '*.og.go'" +
          " ! -regex '.*lang/[^/].*\\.og\\.go$'" +
          " -exec rm {} +",
      ]),
    ]
  )]

_optgen_srcs = rule(
   implementation = _optgen_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(allow_files=True, default=OPTGEN_SRCS),
   },
)

def _stringer_srcs_impl(ctx):
  return [GeneratedFileInfo(
    generated_files = {
      "": [f for di in ctx.attr._srcs for f in di[DefaultInfo].files.to_list()],
    },
  )]

_stringer_srcs = rule(
   implementation = _stringer_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(allow_files=True, default=STRINGER_SRCS),
   },
)

def _docs_srcs_impl(ctx):
  return [GeneratedFileInfo(
    generated_files = {
      "": [f for di in ctx.attr._srcs for f in di[DefaultInfo].files.to_list()],
    },
  )]

_docs_srcs = rule(
   implementation = _docs_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(allow_files=True, default=DOCS_SRCS),
   },
)

def _misc_srcs_impl(ctx):
  return [GeneratedFileInfo(
    generated_files = {
      "": [f for di in ctx.attr._srcs for f in di[DefaultInfo].files.to_list()],
    },
  )]

_misc_srcs = rule(
   implementation = _misc_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(allow_files=True, default=MISC_SRCS),
   },
)



def _hoist_files_impl(ctx):
  cp_file_cmd_fmt = """\
cp {src} {dst}
chmod 0644 {dst}\
"""

  script_fmt = """\
#!/bin/bash
set -euo pipefail

{cleanup_tasks}
{cmds}\
"""
  cleanup_tasks = []
  cmds = []
  generated_files = {}
  for set in ctx.attr.data:
    gfi = set[GeneratedFileInfo]
    cleanup_tasks += gfi.cleanup_tasks if hasattr(gfi, "cleanup_tasks") else []
    for prefix, files in gfi.generated_files.items():
      if prefix not in generated_files:
        generated_files[prefix] = []
      for file in files:
        dst = '"${{BUILD_WORKSPACE_DIRECTORY}}/{}"'.format(
          file.short_path[len(prefix):]
        )
        cmd = cp_file_cmd_fmt.format(src=file.short_path, dst=dst)
        cmds.append(cmd)
        generated_files[prefix].append(file)

  executable = ctx.actions.declare_file(ctx.label.name)
  script = script_fmt.format(
      cleanup_tasks = "\n".join(cleanup_tasks),
      cmds = "\n".join(cmds),
  )
  ctx.actions.write(executable, script, is_executable=True)
  runfiles = ctx.runfiles(files = [file for files in generated_files.values() for file in files])
  return [
    DefaultInfo(executable = executable, runfiles=runfiles),
    GeneratedFileInfo(
      generated_files = generated_files,
      cleanup_tasks = cleanup_tasks,
    )
  ]

_hoist_files = rule(
    implementation = _hoist_files_impl,
    attrs = {
        "data": attr.label_list(providers = [GeneratedFileInfo]),
    },
    executable = True,
)

def _hoist(name, src_rule):
  src_name = name + "_srcs"
  src_rule(name = src_name)
  _hoist_files(name = name, data = [src_name], tags = ["no-remote-exec"])

def gomock():
  _hoist("gomock", _gomock_srcs)

def go_proto():
  _hoist("go_proto", _go_proto_srcs)

def execgen():
  _hoist("execgen", _execgen_srcs)

def stringer():
  _hoist("stringer", _stringer_srcs)

def optgen():
  _hoist("optgen", _optgen_srcs)

def misc():
  _hoist("misc", _misc_srcs)

def docs():
  _hoist("docs", _docs_srcs)

def gen(name, srcs):
  _hoist_files(name = name, data = srcs, tags = ["no-remote-exec"])
