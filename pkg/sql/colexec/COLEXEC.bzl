def _generate_impl(ctx):
    output_files, input_files = [], []
    # Gather the full list of all input template files (deconstructing it from
    # the filegroup input). See [1] for a reference of all types/members within
    # skylark.
    #
    # [1]: https://docs.bazel.build/versions/master/skylark/lib/skylark-overview.html
    for tmpl in ctx.attr.templates:
        input_files.extend(tmpl.files.to_list())

    execgen_binary = ctx.executable.execgen
    goimports_binary = ctx.executable.goimports
    input_binaries = [execgen_binary, goimports_binary]

    # For each target, run execgen and goimports.
    for generated_file_name in ctx.attr.targets:
        generated_file = ctx.actions.declare_file(generated_file_name)
        ctx.actions.run_shell(
            tools = input_binaries,
            inputs = input_files,
            outputs = [generated_file],
            progress_message = "Generating pkg/cmd/colexec/%s" % generated_file_name,
            command = """
            %s -fmt=false pkg/sql/colexec/%s > %s
            %s -w %s
            """ % (execgen_binary.path, generated_file_name, generated_file.path, goimports_binary.path, generated_file.path),
        )
        output_files.append(generated_file)

    # Construct the full set of output files for bazel to be able to analyse.
    # See https://docs.bazel.build/versions/master/skylark/lib/DefaultInfo.html.
    return [DefaultInfo(files = depset(output_files))]

generate = rule(
    implementation = _generate_impl,
    # Source all the necessary template files, output targets, the execgen and goimports binaries.
    attrs = {
        "templates": attr.label_list(mandatory = True, allow_files = ["tmpl.go"]),
        "targets": attr.string_list(mandatory = True),
        "execgen": attr.label(mandatory = True, executable = True, allow_files = True, cfg = "exec"),
        "goimports": attr.label(mandatory = True, executable = True, allow_files = True, cfg = "exec"),
    },
)
