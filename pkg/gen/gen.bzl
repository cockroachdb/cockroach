load("@io_bazel_rules_go//go:def.bzl", "GoSource")

GeneratedFileInfo = provider(
  "Info needed to hoist generated files",
  fields = {
    "generated_files": "dictionary from prefix to list of files",
  }
)

def _go_proto_srcs_impl(ctx):
  generated_files = {}
  for s in ctx.attr.srcs:
    srcs = s[GoSource]
    lbl = srcs.library.label
    imp = srcs.library.importpath
    imp = imp[:imp.find(lbl.package)]
    prefix = "{}/{}_/{}".format(lbl.package, lbl.name, imp)
    generated_files[prefix] = [f for f in srcs.srcs]
  return [
    GeneratedFileInfo(generated_files = generated_files)
  ]

go_proto_srcs = rule(
   implementation = _go_proto_srcs_impl,
   attrs = {
       "srcs": attr.label_list(providers = [GoSource]),
   },
)

_cp_file_cmd_fmt = """
cp {src} {dst}
chmod +w {dst}
"""

_script_fmt = """#!/bin/bash
set -euo pipefail

{}
"""

def _hoist_files_impl(ctx):
    cmds = []
    generated_files = {}
    for set in ctx.attr.data:
      for prefix, files in set[GeneratedFileInfo].generated_files.items():
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
    script = _script_fmt.format("\n".join(cmds))
    ctx.actions.write(executable, script, is_executable=True)
    runfiles = ctx.runfiles(files = [file for files in generated_files.values() for file in files])

    return [
      DefaultInfo(executable = executable, runfiles=runfiles),
      GeneratedFileInfo(generated_files = generated_files)
    ]

hoist_files = rule(
    implementation = _hoist_files_impl,
    attrs = {
        "data": attr.label_list(providers = [GeneratedFileInfo]),
    },
    executable = True,
)
