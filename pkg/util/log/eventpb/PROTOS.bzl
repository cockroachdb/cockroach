# NOTE(ricky): Unfortunately multiple doc rules need this list of protos in a
# very specific order or else builds will fail or produce incorrect results. I
# capture that list here so we only have one thing to update when a new proto is
# added to this directory.

EVENTPB_PROTO_LOCATIONS = """$(location //pkg/util/log/eventpb:events.proto) $(location //pkg/util/log/eventpb:ddl_events.proto) $(location //pkg/util/log/eventpb:misc_sql_events.proto) $(location //pkg/util/log/eventpb:privilege_events.proto) $(location //pkg/util/log/eventpb:role_events.proto) $(location //pkg/util/log/eventpb:zone_events.proto) $(location //pkg/util/log/eventpb:session_events.proto) $(location //pkg/util/log/eventpb:sql_audit_events.proto) $(location //pkg/util/log/eventpb:cluster_events.proto) $(location //pkg/util/log/eventpb:job_events.proto) $(location //pkg/util/log/eventpb:health_events.proto)"""
