#!/usr/bin/env sh

set -eu

rm -rf {,libprotoc/}*.{h,cc} internal
mkdir -p internal

curl -sL https://github.com/google/protobuf/releases/download/v3.0.0-beta-2/protobuf-cpp-3.0.0-beta-2.tar.gz | tar vzxf - -C internal/ --strip-components=1
(cd internal && ./configure)

# protobuf_lite + protobuf from internal/BUILD
PROTOBUF_SOURCES='
src/google/protobuf/arena.cc
src/google/protobuf/arenastring.cc
src/google/protobuf/extension_set.cc
src/google/protobuf/generated_message_util.cc
src/google/protobuf/io/coded_stream.cc
src/google/protobuf/io/zero_copy_stream.cc
src/google/protobuf/io/zero_copy_stream_impl_lite.cc
src/google/protobuf/message_lite.cc
src/google/protobuf/repeated_field.cc
src/google/protobuf/stubs/atomicops_internals_x86_gcc.cc
src/google/protobuf/stubs/atomicops_internals_x86_msvc.cc
src/google/protobuf/stubs/bytestream.cc
src/google/protobuf/stubs/common.cc
src/google/protobuf/stubs/int128.cc
src/google/protobuf/stubs/once.cc
src/google/protobuf/stubs/status.cc
src/google/protobuf/stubs/statusor.cc
src/google/protobuf/stubs/stringpiece.cc
src/google/protobuf/stubs/stringprintf.cc
src/google/protobuf/stubs/structurally_valid.cc
src/google/protobuf/stubs/strutil.cc
src/google/protobuf/stubs/time.cc
src/google/protobuf/wire_format_lite.cc
src/google/protobuf/any.cc
src/google/protobuf/any.pb.cc
src/google/protobuf/api.pb.cc
src/google/protobuf/compiler/importer.cc
src/google/protobuf/compiler/parser.cc
src/google/protobuf/descriptor.cc
src/google/protobuf/descriptor.pb.cc
src/google/protobuf/descriptor_database.cc
src/google/protobuf/duration.pb.cc
src/google/protobuf/dynamic_message.cc
src/google/protobuf/empty.pb.cc
src/google/protobuf/extension_set_heavy.cc
src/google/protobuf/field_mask.pb.cc
src/google/protobuf/generated_message_reflection.cc
src/google/protobuf/io/gzip_stream.cc
src/google/protobuf/io/printer.cc
src/google/protobuf/io/strtod.cc
src/google/protobuf/io/tokenizer.cc
src/google/protobuf/io/zero_copy_stream_impl.cc
src/google/protobuf/map_field.cc
src/google/protobuf/message.cc
src/google/protobuf/reflection_ops.cc
src/google/protobuf/service.cc
src/google/protobuf/source_context.pb.cc
src/google/protobuf/struct.pb.cc
src/google/protobuf/stubs/mathlimits.cc
src/google/protobuf/stubs/substitute.cc
src/google/protobuf/text_format.cc
src/google/protobuf/timestamp.pb.cc
src/google/protobuf/type.pb.cc
src/google/protobuf/unknown_field_set.cc
src/google/protobuf/util/field_comparator.cc
src/google/protobuf/util/field_mask_util.cc
src/google/protobuf/util/internal/datapiece.cc
src/google/protobuf/util/internal/default_value_objectwriter.cc
src/google/protobuf/util/internal/error_listener.cc
src/google/protobuf/util/internal/field_mask_utility.cc
src/google/protobuf/util/internal/json_escaping.cc
src/google/protobuf/util/internal/json_objectwriter.cc
src/google/protobuf/util/internal/json_stream_parser.cc
src/google/protobuf/util/internal/object_writer.cc
src/google/protobuf/util/internal/proto_writer.cc
src/google/protobuf/util/internal/protostream_objectsource.cc
src/google/protobuf/util/internal/protostream_objectwriter.cc
src/google/protobuf/util/internal/type_info.cc
src/google/protobuf/util/internal/type_info_test_helper.cc
src/google/protobuf/util/internal/utility.cc
src/google/protobuf/util/json_util.cc
src/google/protobuf/util/message_differencer.cc
src/google/protobuf/util/time_util.cc
src/google/protobuf/util/type_resolver_util.cc
src/google/protobuf/wire_format.cc
src/google/protobuf/wrappers.pb.cc
'

# symlink so cgo compiles them
for file in $PROTOBUF_SOURCES; do
  ln -sf internal/$file .
done

# copy generated files we want to keep
cp internal/config.h .

# protoc_lib from internal/BUILD
PROTOC_SOURCES='
src/google/protobuf/compiler/code_generator.cc
src/google/protobuf/compiler/command_line_interface.cc
src/google/protobuf/compiler/cpp/cpp_enum.cc
src/google/protobuf/compiler/cpp/cpp_enum_field.cc
src/google/protobuf/compiler/cpp/cpp_extension.cc
src/google/protobuf/compiler/cpp/cpp_field.cc
src/google/protobuf/compiler/cpp/cpp_file.cc
src/google/protobuf/compiler/cpp/cpp_generator.cc
src/google/protobuf/compiler/cpp/cpp_helpers.cc
src/google/protobuf/compiler/cpp/cpp_map_field.cc
src/google/protobuf/compiler/cpp/cpp_message.cc
src/google/protobuf/compiler/cpp/cpp_message_field.cc
src/google/protobuf/compiler/cpp/cpp_primitive_field.cc
src/google/protobuf/compiler/cpp/cpp_service.cc
src/google/protobuf/compiler/cpp/cpp_string_field.cc
src/google/protobuf/compiler/csharp/csharp_doc_comment.cc
src/google/protobuf/compiler/csharp/csharp_enum.cc
src/google/protobuf/compiler/csharp/csharp_enum_field.cc
src/google/protobuf/compiler/csharp/csharp_field_base.cc
src/google/protobuf/compiler/csharp/csharp_generator.cc
src/google/protobuf/compiler/csharp/csharp_helpers.cc
src/google/protobuf/compiler/csharp/csharp_map_field.cc
src/google/protobuf/compiler/csharp/csharp_message.cc
src/google/protobuf/compiler/csharp/csharp_message_field.cc
src/google/protobuf/compiler/csharp/csharp_primitive_field.cc
src/google/protobuf/compiler/csharp/csharp_reflection_class.cc
src/google/protobuf/compiler/csharp/csharp_repeated_enum_field.cc
src/google/protobuf/compiler/csharp/csharp_repeated_message_field.cc
src/google/protobuf/compiler/csharp/csharp_repeated_primitive_field.cc
src/google/protobuf/compiler/csharp/csharp_source_generator_base.cc
src/google/protobuf/compiler/csharp/csharp_wrapper_field.cc
src/google/protobuf/compiler/java/java_context.cc
src/google/protobuf/compiler/java/java_doc_comment.cc
src/google/protobuf/compiler/java/java_enum.cc
src/google/protobuf/compiler/java/java_enum_field.cc
src/google/protobuf/compiler/java/java_enum_field_lite.cc
src/google/protobuf/compiler/java/java_enum_lite.cc
src/google/protobuf/compiler/java/java_extension.cc
src/google/protobuf/compiler/java/java_field.cc
src/google/protobuf/compiler/java/java_file.cc
src/google/protobuf/compiler/java/java_generator.cc
src/google/protobuf/compiler/java/java_generator_factory.cc
src/google/protobuf/compiler/java/java_helpers.cc
src/google/protobuf/compiler/java/java_lazy_message_field.cc
src/google/protobuf/compiler/java/java_lazy_message_field_lite.cc
src/google/protobuf/compiler/java/java_map_field.cc
src/google/protobuf/compiler/java/java_map_field_lite.cc
src/google/protobuf/compiler/java/java_message.cc
src/google/protobuf/compiler/java/java_message_builder.cc
src/google/protobuf/compiler/java/java_message_builder_lite.cc
src/google/protobuf/compiler/java/java_message_field.cc
src/google/protobuf/compiler/java/java_message_field_lite.cc
src/google/protobuf/compiler/java/java_message_lite.cc
src/google/protobuf/compiler/java/java_name_resolver.cc
src/google/protobuf/compiler/java/java_primitive_field.cc
src/google/protobuf/compiler/java/java_primitive_field_lite.cc
src/google/protobuf/compiler/java/java_service.cc
src/google/protobuf/compiler/java/java_shared_code_generator.cc
src/google/protobuf/compiler/java/java_string_field.cc
src/google/protobuf/compiler/java/java_string_field_lite.cc
src/google/protobuf/compiler/javanano/javanano_enum.cc
src/google/protobuf/compiler/javanano/javanano_enum_field.cc
src/google/protobuf/compiler/javanano/javanano_extension.cc
src/google/protobuf/compiler/javanano/javanano_field.cc
src/google/protobuf/compiler/javanano/javanano_file.cc
src/google/protobuf/compiler/javanano/javanano_generator.cc
src/google/protobuf/compiler/javanano/javanano_helpers.cc
src/google/protobuf/compiler/javanano/javanano_map_field.cc
src/google/protobuf/compiler/javanano/javanano_message.cc
src/google/protobuf/compiler/javanano/javanano_message_field.cc
src/google/protobuf/compiler/javanano/javanano_primitive_field.cc
src/google/protobuf/compiler/js/js_generator.cc
src/google/protobuf/compiler/objectivec/objectivec_enum.cc
src/google/protobuf/compiler/objectivec/objectivec_enum_field.cc
src/google/protobuf/compiler/objectivec/objectivec_extension.cc
src/google/protobuf/compiler/objectivec/objectivec_field.cc
src/google/protobuf/compiler/objectivec/objectivec_file.cc
src/google/protobuf/compiler/objectivec/objectivec_generator.cc
src/google/protobuf/compiler/objectivec/objectivec_helpers.cc
src/google/protobuf/compiler/objectivec/objectivec_map_field.cc
src/google/protobuf/compiler/objectivec/objectivec_message.cc
src/google/protobuf/compiler/objectivec/objectivec_message_field.cc
src/google/protobuf/compiler/objectivec/objectivec_oneof.cc
src/google/protobuf/compiler/objectivec/objectivec_primitive_field.cc
src/google/protobuf/compiler/plugin.cc
src/google/protobuf/compiler/plugin.pb.cc
src/google/protobuf/compiler/python/python_generator.cc
src/google/protobuf/compiler/ruby/ruby_generator.cc
src/google/protobuf/compiler/subprocess.cc
src/google/protobuf/compiler/zip_writer.cc
'

# symlink so cgo compiles them
for file in $PROTOC_SOURCES; do
  ln -sf ../internal/$file libprotoc
done
