load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary", "npm_package_bin")

def pbjs(**kwargs):
    output_dir = kwargs.pop("output_dir", False)
    if "outs" in kwargs or output_dir:
        npm_package_bin(tool = "//pkg/ui/build/protobufjs/bin:pbjs", output_dir = output_dir, **kwargs)
    else:
        nodejs_binary(
            entry_point = "//pkg/ui:node_modules/protobufjs/bin/pbjs",
            data = ["//pkg/ui/build/protobufjs:protobufjs"] + kwargs.pop("data", []),
            **kwargs
        )

def pbts(**kwargs):
    output_dir = kwargs.pop("output_dir", False)
    if "outs" in kwargs or output_dir:
        npm_package_bin(tool = "//pkg/ui/build/protobufjs/bin:pbts", output_dir = output_dir, **kwargs)
    else:
        nodejs_binary(
            entry_point = "//pkg/ui:node_modules/protobufjs/bin/pbts",
            data = ["//pkg/ui/build/protobufjs:protobufjs"] + kwargs.pop("data", []),
            **kwargs
        )
