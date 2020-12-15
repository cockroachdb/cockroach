load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

def proto_lib(**kwargs):
    """
    Defines a proto library similarly to the standard proto_library rule.

    https://docs.bazel.build/versions/master/be/protocol-buffer.html#proto_library
    """
    deps = kwargs.get('deps', [])
    deps += [
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:any_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:api_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:descriptor_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:duration_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:empty_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:field_mask_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:source_context_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:struct_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:timestamp_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:type_proto",
        "//vendor/github.com/gogo/protobuf/protobuf/google/protobuf:wrappers_proto",
    ]
    kwargs['deps'] = deps
    proto_library(**kwargs)

def go_proto_lib(**kwargs):
    """
    Defines a proto library similarly to the go_proto_library rule.

    https://github.com/bazelbuild/rules_go/blob/master/proto/core.rst#go-proto-library
    """
    go_proto_library(
        compilers = [
            "//pkg/cmd/protoc-gen-gogoroach:protoc-gen-gogoroach_compiler",
        ],
        **kwargs,
    )
