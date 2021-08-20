# NOTE(ricky): Unfortunately multiple doc rules need this list of protos in a
# very specific order or else builds will fail or produce incorrect results. I
# capture that list here so we only have one thing to update when a new proto is
# added to this directory.

# Add new .proto files to this list. Order matters. Keep in sync with the `Makefile`.
EVENTPB_PROTOS = [
    "events.proto",
    "ddl_events.proto",
    "misc_sql_events.proto",
    "privilege_events.proto",
    "role_events.proto",
    "zone_events.proto",
    "session_events.proto",
    "sql_audit_events.proto",
    "cluster_events.proto",
    "job_events.proto",
    "health_events.proto",
]

# The same list as above, but formatted such that outside Bazel rules can depend
# on them as `srcs`.
EVENTPB_PROTO_SRCS = ["//pkg/util/log/eventpb:{}".format(proto) for proto in EVENTPB_PROTOS]

# The $(location) of each of these .protos in the order above.
EVENTPB_PROTO_LOCATIONS = " ".join(["$(location {})".format(src) for src in EVENTPB_PROTO_SRCS])
