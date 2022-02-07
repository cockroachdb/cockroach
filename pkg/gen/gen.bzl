load("@io_bazel_rules_go//go:def.bzl", "GoSource")
load(":protobuf_srcs.bzl", "protobuf_srcs")

GeneratedFileInfo = provider(
  "Info needed to hoist generated files",
  fields = {
    "generated_files": "dictionary from prefix to list of files",
    "cleanup_tasks": "list of bash commands to run"
  }
)

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
# Use a subshell with () to avoid changing the directory in the main shell.
"""
(
    cd "${BUILD_WORKSPACE_DIRECTORY}"
    # Avoid searching the node_modules directory because it's full of
    # irrelevant files.
    find ./pkg -name node_modules -prune -o \
        -type f -name '*.pb.go' -exec rm {} + -o \
        -type f -name '*.pb.gw.go' -exec rm {} +
)
""",
      ],
    )
  ]

go_proto_srcs = rule(
   implementation = _go_proto_srcs_impl,
   attrs = {
       "_srcs": attr.label_list(providers = [GoSource], default=protobuf_srcs),
   },
)

_cp_file_cmd_fmt = """
cp {src} {dst}
chmod +w {dst}
"""

_script_fmt = """#!/bin/bash
set -euo pipefail

{cleanup_tasks}
{cmds}
"""

def _hoist_files_impl(ctx):
    cleanup_tasks = []
    cmds = []
    generated_files = {}
    for set in ctx.attr.data:
      gfi = set[GeneratedFileInfo]
      cleanup_tasks += gfi.cleanup_tasks
      for prefix, files in gfi.generated_files.items():
        if prefix not in generated_files:
          generated_files[prefix] = []
        for file in files:
          dst = '"${{BUILD_WORKSPACE_DIRECTORY}}/{}"'.format(
            file.short_path[len(prefix):]
          )
          cmd = _cp_file_cmd_fmt.format(src=file.short_path, dst=dst)
          cmds.append(cmd)
          generated_files[prefix].append(file)

    executable = ctx.actions.declare_file(ctx.label.name)
    script = _script_fmt.format(
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

hoist_files = rule(
    implementation = _hoist_files_impl,
    attrs = {
        "data": attr.label_list(providers = [GeneratedFileInfo]),
    },
    executable = True,
)
