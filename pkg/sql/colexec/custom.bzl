def _generate_impl(ctx):
    output_files, input_files = [], []
    for tmpl in ctx.attr.templates:
        input_files.extend(tmpl.files.to_list())

    for generated_file_name in ctx.attr.targets:
        generated_file = ctx.actions.declare_file(generated_file_name)
        execgen_binary = ctx.executable.binary
        goimports_binary = ctx.executable.goimports
        ctx.actions.run_shell(
            tools = [execgen_binary, goimports_binary],
            inputs = input_files,
            outputs = [generated_file],
            progress_message = "Generating pkg/cmd/colexec/%s" % generated_file_name,
            command = """
            %s -fmt=false pkg/sql/colexec/%s > %s
            %s -w %s
            """ % (execgen_binary.path, generated_file_name, generated_file.path, goimports_binary.path, generated_file.path),
        )
        output_files.append(generated_file)

    return [DefaultInfo(files = depset(output_files))]

generate = rule(
    implementation = _generate_impl,
    attrs = {
        "templates": attr.label_list(mandatory = True, allow_files = ["tmpl.go"]),
        "targets": attr.string_list(mandatory = True),
        "binary": attr.label(mandatory = True, executable = True, allow_files = True, cfg = "exec"),
        "goimports": attr.label(mandatory = True, executable = True, allow_files = True, cfg = "exec"),
    },
)
