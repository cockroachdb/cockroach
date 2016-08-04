// GENERATED FILE DO NOT EDIT
module.exports = require("protobufjs").newBuilder({})['import']({
    "package": "cockroach",
    "messages": [
        {
            "name": "util",
            "fields": [],
            "options": {
                "go_package": "util"
            },
            "messages": [
                {
                    "name": "UnresolvedAddr",
                    "options": {
                        "(gogoproto.goproto_stringer)": false
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "network_field",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "address_field",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "hlc",
                    "fields": [],
                    "options": {
                        "go_package": "hlc"
                    },
                    "messages": [
                        {
                            "name": "Timestamp",
                            "options": {
                                "(gogoproto.goproto_stringer)": false,
                                "(gogoproto.populate)": true
                            },
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "wall_time",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "int32",
                                    "name": "logical",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "roachpb",
            "fields": [],
            "options": {
                "go_package": "roachpb"
            },
            "messages": [
                {
                    "name": "Attributes",
                    "options": {
                        "(gogoproto.goproto_stringer)": false
                    },
                    "fields": [
                        {
                            "rule": "repeated",
                            "type": "string",
                            "name": "attrs",
                            "id": 1,
                            "options": {
                                "(gogoproto.moretags)": "yaml:\\\"attrs,flow\\\""
                            }
                        }
                    ]
                },
                {
                    "name": "ReplicaDescriptor",
                    "options": {
                        "(gogoproto.populate)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "node_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "NodeID",
                                "(gogoproto.casttype)": "NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "store_id",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "StoreID",
                                "(gogoproto.casttype)": "StoreID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "replica_id",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "ReplicaID",
                                "(gogoproto.casttype)": "ReplicaID"
                            }
                        }
                    ]
                },
                {
                    "name": "ReplicaIdent",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "range_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "RangeID",
                                "(gogoproto.casttype)": "RangeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "ReplicaDescriptor",
                            "name": "replica",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "RangeDescriptor",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "range_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "RangeID",
                                "(gogoproto.casttype)": "RangeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "start_key",
                            "id": 2,
                            "options": {
                                "(gogoproto.casttype)": "RKey"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "end_key",
                            "id": 3,
                            "options": {
                                "(gogoproto.casttype)": "RKey"
                            }
                        },
                        {
                            "rule": "repeated",
                            "type": "ReplicaDescriptor",
                            "name": "replicas",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "next_replica_id",
                            "id": 5,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "NextReplicaID",
                                "(gogoproto.casttype)": "ReplicaID"
                            }
                        }
                    ]
                },
                {
                    "name": "StoreCapacity",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "capacity",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "available",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "range_count",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "NodeDescriptor",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "node_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "NodeID",
                                "(gogoproto.casttype)": "NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.UnresolvedAddr",
                            "name": "address",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "Attributes",
                            "name": "attrs",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "StoreDescriptor",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "store_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "StoreID",
                                "(gogoproto.casttype)": "StoreID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "Attributes",
                            "name": "attrs",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "NodeDescriptor",
                            "name": "node",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "StoreCapacity",
                            "name": "capacity",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "StoreDeadReplicas",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "store_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "StoreID",
                                "(gogoproto.casttype)": "StoreID"
                            }
                        },
                        {
                            "rule": "repeated",
                            "type": "ReplicaIdent",
                            "name": "replicas",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "Span",
                    "options": {
                        "(gogoproto.populate)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "key",
                            "id": 3,
                            "options": {
                                "(gogoproto.casttype)": "Key"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "end_key",
                            "id": 4,
                            "options": {
                                "(gogoproto.casttype)": "Key"
                            }
                        }
                    ]
                },
                {
                    "name": "Value",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "raw_bytes",
                            "id": 1
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "timestamp",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "KeyValue",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "key",
                            "id": 1,
                            "options": {
                                "(gogoproto.casttype)": "Key"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "Value",
                            "name": "value",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "StoreIdent",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "cluster_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "ClusterID",
                                "(gogoproto.customtype)": "github.com/cockroachdb/cockroach/util/uuid.UUID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "node_id",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "NodeID",
                                "(gogoproto.casttype)": "NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "store_id",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "StoreID",
                                "(gogoproto.casttype)": "StoreID"
                            }
                        }
                    ]
                },
                {
                    "name": "SplitTrigger",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "RangeDescriptor",
                            "name": "left_desc",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "RangeDescriptor",
                            "name": "right_desc",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "MergeTrigger",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "RangeDescriptor",
                            "name": "left_desc",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "RangeDescriptor",
                            "name": "right_desc",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "ChangeReplicasTrigger",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "ReplicaChangeType",
                            "name": "change_type",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "ReplicaDescriptor",
                            "name": "replica",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "repeated",
                            "type": "ReplicaDescriptor",
                            "name": "updated_replicas",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "next_replica_id",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "NextReplicaID",
                                "(gogoproto.casttype)": "ReplicaID"
                            }
                        }
                    ]
                },
                {
                    "name": "ModifiedSpanTrigger",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "bool",
                            "name": "system_config_span",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "InternalCommitTrigger",
                    "options": {
                        "(gogoproto.goproto_getters)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "SplitTrigger",
                            "name": "split_trigger",
                            "id": 1
                        },
                        {
                            "rule": "optional",
                            "type": "MergeTrigger",
                            "name": "merge_trigger",
                            "id": 2
                        },
                        {
                            "rule": "optional",
                            "type": "ChangeReplicasTrigger",
                            "name": "change_replicas_trigger",
                            "id": 3
                        },
                        {
                            "rule": "optional",
                            "type": "ModifiedSpanTrigger",
                            "name": "modified_span_trigger",
                            "id": 4
                        }
                    ]
                },
                {
                    "name": "Transaction",
                    "options": {
                        "(gogoproto.goproto_stringer)": false,
                        "(gogoproto.populate)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "storage.engine.enginepb.TxnMeta",
                            "name": "meta",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.embed)": true
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "name",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "TransactionStatus",
                            "name": "status",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "last_heartbeat",
                            "id": 5
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "orig_timestamp",
                            "id": 6,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "max_timestamp",
                            "id": 7,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "map",
                            "type": "util.hlc.Timestamp",
                            "keytype": "int32",
                            "name": "observed_timestamps",
                            "id": 8,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.castkey)": "NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "bool",
                            "name": "writing",
                            "id": 9,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "bool",
                            "name": "write_too_old",
                            "id": 12,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "bool",
                            "name": "retry_on_push",
                            "id": 13,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "repeated",
                            "type": "Span",
                            "name": "intents",
                            "id": 11,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "Intent",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "Span",
                            "name": "span",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.embed)": true
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "storage.engine.enginepb.TxnMeta",
                            "name": "txn",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "TransactionStatus",
                            "name": "status",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "Lease",
                    "options": {
                        "(gogoproto.goproto_stringer)": false,
                        "(gogoproto.populate)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "start",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "start_stasis",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "expiration",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "ReplicaDescriptor",
                            "name": "replica",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "AbortCacheEntry",
                    "options": {
                        "(gogoproto.populate)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "bytes",
                            "name": "key",
                            "id": 1,
                            "options": {
                                "(gogoproto.casttype)": "Key"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "timestamp",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "priority",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "RaftTruncatedState",
                    "options": {
                        "(gogoproto.populate)": true
                    },
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "uint64",
                            "name": "index",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "uint64",
                            "name": "term",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "RaftTombstone",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "next_replica_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "NextReplicaID",
                                "(gogoproto.casttype)": "ReplicaID"
                            }
                        }
                    ]
                },
                {
                    "name": "RaftSnapshotData",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "RangeDescriptor",
                            "name": "range_descriptor",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "repeated",
                            "type": "KeyValue",
                            "name": "KV",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "KV"
                            }
                        },
                        {
                            "rule": "repeated",
                            "type": "bytes",
                            "name": "log_entries",
                            "id": 3
                        }
                    ],
                    "messages": [
                        {
                            "name": "KeyValue",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "bytes",
                                    "name": "key",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "bytes",
                                    "name": "value",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "util.hlc.Timestamp",
                                    "name": "timestamp",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],
            "enums": [
                {
                    "name": "ValueType",
                    "values": [
                        {
                            "name": "UNKNOWN",
                            "id": 0
                        },
                        {
                            "name": "NULL",
                            "id": 7
                        },
                        {
                            "name": "INT",
                            "id": 1
                        },
                        {
                            "name": "FLOAT",
                            "id": 2
                        },
                        {
                            "name": "BYTES",
                            "id": 3
                        },
                        {
                            "name": "DELIMITED_BYTES",
                            "id": 8
                        },
                        {
                            "name": "TIME",
                            "id": 4
                        },
                        {
                            "name": "DECIMAL",
                            "id": 5
                        },
                        {
                            "name": "DELIMITED_DECIMAL",
                            "id": 9
                        },
                        {
                            "name": "DURATION",
                            "id": 6
                        },
                        {
                            "name": "TUPLE",
                            "id": 10
                        },
                        {
                            "name": "TIMESERIES",
                            "id": 100
                        }
                    ]
                },
                {
                    "name": "ReplicaChangeType",
                    "values": [
                        {
                            "name": "ADD_REPLICA",
                            "id": 0
                        },
                        {
                            "name": "REMOVE_REPLICA",
                            "id": 1
                        }
                    ],
                    "options": {
                        "(gogoproto.goproto_enum_prefix)": false
                    }
                },
                {
                    "name": "TransactionStatus",
                    "values": [
                        {
                            "name": "PENDING",
                            "id": 0
                        },
                        {
                            "name": "COMMITTED",
                            "id": 1
                        },
                        {
                            "name": "ABORTED",
                            "id": 2
                        }
                    ],
                    "options": {
                        "(gogoproto.goproto_enum_prefix)": false
                    }
                }
            ]
        },
        {
            "name": "storage",
            "fields": [],
            "messages": [
                {
                    "name": "engine",
                    "fields": [],
                    "messages": [
                        {
                            "name": "enginepb",
                            "fields": [],
                            "options": {
                                "go_package": "enginepb"
                            },
                            "messages": [
                                {
                                    "name": "TxnMeta",
                                    "options": {
                                        "(gogoproto.populate)": true
                                    },
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "bytes",
                                            "name": "id",
                                            "id": 1,
                                            "options": {
                                                "(gogoproto.customname)": "ID",
                                                "(gogoproto.customtype)": "github.com/cockroachdb/cockroach/util/uuid.UUID"
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "IsolationType",
                                            "name": "isolation",
                                            "id": 2,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bytes",
                                            "name": "key",
                                            "id": 3
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "uint32",
                                            "name": "epoch",
                                            "id": 4,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "util.hlc.Timestamp",
                                            "name": "timestamp",
                                            "id": 5,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int32",
                                            "name": "priority",
                                            "id": 6,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int32",
                                            "name": "sequence",
                                            "id": 7,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int32",
                                            "name": "batch_index",
                                            "id": 8,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        }
                                    ]
                                },
                                {
                                    "name": "MVCCMetadata",
                                    "options": {
                                        "(gogoproto.populate)": true
                                    },
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "TxnMeta",
                                            "name": "txn",
                                            "id": 1
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "util.hlc.Timestamp",
                                            "name": "timestamp",
                                            "id": 2,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bool",
                                            "name": "deleted",
                                            "id": 3,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int64",
                                            "name": "key_bytes",
                                            "id": 4,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int64",
                                            "name": "val_bytes",
                                            "id": 5,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bytes",
                                            "name": "raw_bytes",
                                            "id": 6
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "util.hlc.Timestamp",
                                            "name": "merge_timestamp",
                                            "id": 7
                                        }
                                    ]
                                },
                                {
                                    "name": "MVCCStats",
                                    "options": {
                                        "(gogoproto.populate)": true
                                    },
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "bool",
                                            "name": "contains_estimates",
                                            "id": 14,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "last_update_nanos",
                                            "id": 1,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "intent_age",
                                            "id": 2,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "gc_bytes_age",
                                            "id": 3,
                                            "options": {
                                                "(gogoproto.nullable)": false,
                                                "(gogoproto.customname)": "GCBytesAge"
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "live_bytes",
                                            "id": 4,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "live_count",
                                            "id": 5,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "key_bytes",
                                            "id": 6,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "key_count",
                                            "id": 7,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "val_bytes",
                                            "id": 8,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "val_count",
                                            "id": 9,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "intent_bytes",
                                            "id": 10,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "intent_count",
                                            "id": 11,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "sys_bytes",
                                            "id": 12,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "sfixed64",
                                            "name": "sys_count",
                                            "id": 13,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        }
                                    ]
                                }
                            ],
                            "enums": [
                                {
                                    "name": "IsolationType",
                                    "values": [
                                        {
                                            "name": "SERIALIZABLE",
                                            "id": 0
                                        },
                                        {
                                            "name": "SNAPSHOT",
                                            "id": 1
                                        }
                                    ],
                                    "options": {
                                        "(gogoproto.goproto_enum_prefix)": false
                                    }
                                }
                            ]
                        }
                    ]
                },
                {
                    "name": "storagebase",
                    "fields": [],
                    "options": {
                        "go_package": "storagebase"
                    },
                    "messages": [
                        {
                            "name": "ReplicaState",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "uint64",
                                    "name": "raft_applied_index",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "uint64",
                                    "name": "lease_applied_index",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "roachpb.RangeDescriptor",
                                    "name": "desc",
                                    "id": 3
                                },
                                {
                                    "rule": "optional",
                                    "type": "roachpb.Lease",
                                    "name": "lease",
                                    "id": 4
                                },
                                {
                                    "rule": "optional",
                                    "type": "roachpb.RaftTruncatedState",
                                    "name": "truncated_state",
                                    "id": 5
                                },
                                {
                                    "rule": "optional",
                                    "type": "util.hlc.Timestamp",
                                    "name": "gc_threshold",
                                    "id": 6,
                                    "options": {
                                        "(gogoproto.nullable)": false,
                                        "(gogoproto.customname)": "GCThreshold"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "engine.enginepb.MVCCStats",
                                    "name": "stats",
                                    "id": 7,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "bool",
                                    "name": "frozen",
                                    "id": 8
                                }
                            ]
                        },
                        {
                            "name": "RangeInfo",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "ReplicaState",
                                    "name": "state",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false,
                                        "(gogoproto.embed)": true
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "uint64",
                                    "name": "lastIndex",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "uint64",
                                    "name": "num_pending",
                                    "id": 3
                                },
                                {
                                    "rule": "optional",
                                    "type": "util.hlc.Timestamp",
                                    "name": "last_verification",
                                    "id": 4,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "uint64",
                                    "name": "num_dropped",
                                    "id": 5
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "raft_log_size",
                                    "id": 6
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "config",
            "fields": [],
            "options": {
                "go_package": "config"
            },
            "messages": [
                {
                    "name": "GCPolicy",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "ttl_seconds",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "TTLSeconds"
                            }
                        }
                    ]
                },
                {
                    "name": "ZoneConfig",
                    "fields": [
                        {
                            "rule": "repeated",
                            "type": "roachpb.Attributes",
                            "name": "replica_attrs",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.moretags)": "yaml:\\\"replicas,omitempty\\\""
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "range_min_bytes",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.moretags)": "yaml:\\\"range_min_bytes\\\""
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "range_max_bytes",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.moretags)": "yaml:\\\"range_max_bytes\\\""
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "GCPolicy",
                            "name": "gc",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.customname)": "GC"
                            }
                        }
                    ]
                },
                {
                    "name": "SystemConfig",
                    "fields": [
                        {
                            "rule": "repeated",
                            "type": "roachpb.KeyValue",
                            "name": "values",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                }
            ]
        },
        {
            "name": "server",
            "fields": [],
            "messages": [
                {
                    "name": "serverpb",
                    "fields": [],
                    "options": {
                        "go_package": "serverpb"
                    },
                    "messages": [
                        {
                            "name": "DatabasesRequest",
                            "fields": []
                        },
                        {
                            "name": "DatabasesResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "string",
                                    "name": "databases",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "DatabaseDetailsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "database",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "DatabaseDetailsResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "Grant",
                                    "name": "grants",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "string",
                                    "name": "table_names",
                                    "id": 2
                                }
                            ],
                            "messages": [
                                {
                                    "name": "Grant",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "user",
                                            "id": 1
                                        },
                                        {
                                            "rule": "repeated",
                                            "type": "string",
                                            "name": "privileges",
                                            "id": 2
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "TableDetailsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "database",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "table",
                                    "id": 2
                                }
                            ]
                        },
                        {
                            "name": "TableDetailsResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "Grant",
                                    "name": "grants",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "Column",
                                    "name": "columns",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "Index",
                                    "name": "indexes",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "range_count",
                                    "id": 4
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "create_table_statement",
                                    "id": 5
                                },
                                {
                                    "rule": "optional",
                                    "type": "config.ZoneConfig",
                                    "name": "zone_config",
                                    "id": 6,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "ZoneConfigurationLevel",
                                    "name": "zone_config_level",
                                    "id": 7
                                }
                            ],
                            "messages": [
                                {
                                    "name": "Grant",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "user",
                                            "id": 1
                                        },
                                        {
                                            "rule": "repeated",
                                            "type": "string",
                                            "name": "privileges",
                                            "id": 2
                                        }
                                    ]
                                },
                                {
                                    "name": "Column",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "name",
                                            "id": 1
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "type",
                                            "id": 2
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bool",
                                            "name": "nullable",
                                            "id": 3
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "default_value",
                                            "id": 4
                                        }
                                    ]
                                },
                                {
                                    "name": "Index",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "name",
                                            "id": 1
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bool",
                                            "name": "unique",
                                            "id": 2
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int64",
                                            "name": "seq",
                                            "id": 3
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "column",
                                            "id": 4
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "direction",
                                            "id": 5
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bool",
                                            "name": "storing",
                                            "id": 6
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "TableStatsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "database",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "table",
                                    "id": 2
                                }
                            ]
                        },
                        {
                            "name": "TableStatsResponse",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "range_count",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "replica_count",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "node_count",
                                    "id": 3
                                },
                                {
                                    "rule": "optional",
                                    "type": "storage.engine.enginepb.MVCCStats",
                                    "name": "stats",
                                    "id": 4,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "MissingNode",
                                    "name": "missing_nodes",
                                    "id": 5,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ],
                            "messages": [
                                {
                                    "name": "MissingNode",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "node_id",
                                            "id": 1,
                                            "options": {
                                                "(gogoproto.customname)": "NodeID"
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "error_message",
                                            "id": 2
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "UsersRequest",
                            "fields": []
                        },
                        {
                            "name": "UsersResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "User",
                                    "name": "users",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ],
                            "messages": [
                                {
                                    "name": "User",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "username",
                                            "id": 1
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "EventsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "type",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "target_id",
                                    "id": 2
                                }
                            ]
                        },
                        {
                            "name": "EventsResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "Event",
                                    "name": "events",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ],
                            "messages": [
                                {
                                    "name": "Event",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "Timestamp",
                                            "name": "timestamp",
                                            "id": 1,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "event_type",
                                            "id": 2
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int64",
                                            "name": "target_id",
                                            "id": 3,
                                            "options": {
                                                "(gogoproto.customname)": "TargetID"
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "int64",
                                            "name": "reporting_id",
                                            "id": 4,
                                            "options": {
                                                "(gogoproto.customname)": "ReportingID"
                                            }
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "string",
                                            "name": "info",
                                            "id": 5
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "bytes",
                                            "name": "unique_id",
                                            "id": 6,
                                            "options": {
                                                "(gogoproto.customname)": "UniqueID"
                                            }
                                        }
                                    ],
                                    "messages": [
                                        {
                                            "name": "Timestamp",
                                            "fields": [
                                                {
                                                    "rule": "optional",
                                                    "type": "int64",
                                                    "name": "sec",
                                                    "id": 1
                                                },
                                                {
                                                    "rule": "optional",
                                                    "type": "uint32",
                                                    "name": "nsec",
                                                    "id": 2
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "SetUIDataRequest",
                            "fields": [
                                {
                                    "rule": "map",
                                    "type": "bytes",
                                    "keytype": "string",
                                    "name": "key_values",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "SetUIDataResponse",
                            "fields": []
                        },
                        {
                            "name": "GetUIDataRequest",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "string",
                                    "name": "keys",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "GetUIDataResponse",
                            "fields": [
                                {
                                    "rule": "map",
                                    "type": "Value",
                                    "keytype": "string",
                                    "name": "key_values",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ],
                            "messages": [
                                {
                                    "name": "Timestamp",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "int64",
                                            "name": "sec",
                                            "id": 1
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "uint32",
                                            "name": "nsec",
                                            "id": 2
                                        }
                                    ]
                                },
                                {
                                    "name": "Value",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "bytes",
                                            "name": "value",
                                            "id": 1
                                        },
                                        {
                                            "rule": "optional",
                                            "type": "Timestamp",
                                            "name": "last_updated",
                                            "id": 2,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "ClusterRequest",
                            "fields": []
                        },
                        {
                            "name": "ClusterResponse",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "cluster_id",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.customname)": "ClusterID"
                                    }
                                }
                            ]
                        },
                        {
                            "name": "DrainRequest",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "int32",
                                    "name": "on",
                                    "id": 1
                                },
                                {
                                    "rule": "repeated",
                                    "type": "int32",
                                    "name": "off",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "bool",
                                    "name": "shutdown",
                                    "id": 3
                                }
                            ]
                        },
                        {
                            "name": "DrainResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "int32",
                                    "name": "on",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "HealthRequest",
                            "fields": []
                        },
                        {
                            "name": "HealthResponse",
                            "fields": []
                        },
                        {
                            "name": "ClusterFreezeRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "bool",
                                    "name": "freeze",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "ClusterFreezeResponse",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "ranges_affected",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "message",
                                    "id": 2
                                }
                            ]
                        },
                        {
                            "name": "DetailsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "DetailsResponse",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int32",
                                    "name": "node_id",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.customname)": "NodeID",
                                        "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "util.UnresolvedAddr",
                                    "name": "address",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "build.Info",
                                    "name": "build_info",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "NodesRequest",
                            "fields": []
                        },
                        {
                            "name": "NodesResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "status.NodeStatus",
                                    "name": "nodes",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "NodeRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "RangeInfo",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "PrettySpan",
                                    "name": "span",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "raft_state",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "storage.storagebase.RangeInfo",
                                    "name": "state",
                                    "id": 4,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "RangesRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "RangesResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "RangeInfo",
                                    "name": "ranges",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "GossipRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "JSONResponse",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "bytes",
                                    "name": "data",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "LogsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "level",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "start_time",
                                    "id": 3
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "end_time",
                                    "id": 4
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "max",
                                    "id": 5
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "pattern",
                                    "id": 6
                                }
                            ]
                        },
                        {
                            "name": "LogFilesListRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "LogFileRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "file",
                                    "id": 2
                                }
                            ]
                        },
                        {
                            "name": "StacksRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "MetricsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "RaftRangeNode",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int32",
                                    "name": "node_id",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.customname)": "NodeID",
                                        "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "RangeInfo",
                                    "name": "range",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "RaftRangeError",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "message",
                                    "id": 1
                                }
                            ]
                        },
                        {
                            "name": "RaftRangeStatus",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "range_id",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.customname)": "RangeID",
                                        "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.RangeID"
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "RaftRangeError",
                                    "name": "errors",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "RaftRangeNode",
                                    "name": "nodes",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "RaftDebugRequest",
                            "fields": []
                        },
                        {
                            "name": "RaftDebugResponse",
                            "fields": [
                                {
                                    "rule": "map",
                                    "type": "RaftRangeStatus",
                                    "keytype": "int64",
                                    "name": "ranges",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false,
                                        "(gogoproto.castkey)": "github.com/cockroachdb/cockroach/roachpb.RangeID"
                                    }
                                }
                            ]
                        },
                        {
                            "name": "SpanStatsRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "node_id",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.customname)": "NodeID"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "bytes",
                                    "name": "start_key",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.RKey"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "bytes",
                                    "name": "end_key",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.RKey"
                                    }
                                }
                            ]
                        },
                        {
                            "name": "SpanStatsResponse",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int32",
                                    "name": "range_count",
                                    "id": 2
                                },
                                {
                                    "rule": "optional",
                                    "type": "storage.engine.enginepb.MVCCStats",
                                    "name": "total_stats",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "PrettySpan",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "start_key",
                                    "id": 1
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "end_key",
                                    "id": 2
                                }
                            ]
                        }
                    ],
                    "enums": [
                        {
                            "name": "ZoneConfigurationLevel",
                            "values": [
                                {
                                    "name": "UNKNOWN",
                                    "id": 0
                                },
                                {
                                    "name": "CLUSTER",
                                    "id": 1
                                },
                                {
                                    "name": "DATABASE",
                                    "id": 2
                                },
                                {
                                    "name": "TABLE",
                                    "id": 3
                                }
                            ]
                        },
                        {
                            "name": "DrainMode",
                            "values": [
                                {
                                    "name": "CLIENT",
                                    "id": 0
                                },
                                {
                                    "name": "LEASES",
                                    "id": 1
                                }
                            ]
                        }
                    ],
                    "services": [
                        {
                            "name": "Admin",
                            "options": {},
                            "rpc": {
                                "Users": {
                                    "request": "UsersRequest",
                                    "response": "UsersResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/users"
                                    }
                                },
                                "Databases": {
                                    "request": "DatabasesRequest",
                                    "response": "DatabasesResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/databases"
                                    }
                                },
                                "DatabaseDetails": {
                                    "request": "DatabaseDetailsRequest",
                                    "response": "DatabaseDetailsResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/databases/{database}"
                                    }
                                },
                                "TableDetails": {
                                    "request": "TableDetailsRequest",
                                    "response": "TableDetailsResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/databases/{database}/tables/{table}"
                                    }
                                },
                                "TableStats": {
                                    "request": "TableStatsRequest",
                                    "response": "TableStatsResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/databases/{database}/tables/{table}/stats"
                                    }
                                },
                                "Events": {
                                    "request": "EventsRequest",
                                    "response": "EventsResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/events"
                                    }
                                },
                                "SetUIData": {
                                    "request": "SetUIDataRequest",
                                    "response": "SetUIDataResponse",
                                    "options": {
                                        "(google.api.http).post": "/_admin/v1/uidata",
                                        "(google.api.http).body": "*"
                                    }
                                },
                                "GetUIData": {
                                    "request": "GetUIDataRequest",
                                    "response": "GetUIDataResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/uidata"
                                    }
                                },
                                "Cluster": {
                                    "request": "ClusterRequest",
                                    "response": "ClusterResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/cluster"
                                    }
                                },
                                "Health": {
                                    "request": "HealthRequest",
                                    "response": "HealthResponse",
                                    "options": {
                                        "(google.api.http).get": "/_admin/v1/health"
                                    }
                                },
                                "Drain": {
                                    "request": "DrainRequest",
                                    "response": "DrainResponse",
                                    "options": {
                                        "(google.api.http).post": "/_admin/v1/drain",
                                        "(google.api.http).body": "*"
                                    }
                                },
                                "ClusterFreeze": {
                                    "request": "ClusterFreezeRequest",
                                    "response": "ClusterFreezeResponse",
                                    "options": {
                                        "(google.api.http).post": "/_admin/v1/cluster/freeze",
                                        "(google.api.http).body": "*"
                                    }
                                }
                            }
                        },
                        {
                            "name": "Status",
                            "options": {},
                            "rpc": {
                                "Details": {
                                    "request": "DetailsRequest",
                                    "response": "DetailsResponse",
                                    "options": {
                                        "(google.api.http).get": "/_status/details/{node_id}",
                                        "(google.api.http).additional_bindings.get": "/health"
                                    }
                                },
                                "Nodes": {
                                    "request": "NodesRequest",
                                    "response": "NodesResponse",
                                    "options": {
                                        "(google.api.http).get": "/_status/nodes"
                                    }
                                },
                                "Node": {
                                    "request": "NodeRequest",
                                    "response": "status.NodeStatus",
                                    "options": {
                                        "(google.api.http).get": "/_status/nodes/{node_id}"
                                    }
                                },
                                "RaftDebug": {
                                    "request": "RaftDebugRequest",
                                    "response": "RaftDebugResponse",
                                    "options": {
                                        "(google.api.http).get": "/_status/raft"
                                    }
                                },
                                "Ranges": {
                                    "request": "RangesRequest",
                                    "response": "RangesResponse",
                                    "options": {
                                        "(google.api.http).get": "/_status/ranges/{node_id}"
                                    }
                                },
                                "Gossip": {
                                    "request": "GossipRequest",
                                    "response": "gossip.InfoStatus",
                                    "options": {
                                        "(google.api.http).get": "/_status/gossip/{node_id}"
                                    }
                                },
                                "SpanStats": {
                                    "request": "SpanStatsRequest",
                                    "response": "SpanStatsResponse",
                                    "options": {
                                        "(google.api.http).post": "/_status/span",
                                        "(google.api.http).body": "*"
                                    }
                                },
                                "Stacks": {
                                    "request": "StacksRequest",
                                    "response": "JSONResponse",
                                    "options": {}
                                },
                                "Metrics": {
                                    "request": "MetricsRequest",
                                    "response": "JSONResponse",
                                    "options": {}
                                },
                                "Logs": {
                                    "request": "LogsRequest",
                                    "response": "JSONResponse",
                                    "options": {}
                                },
                                "LogFilesList": {
                                    "request": "LogFilesListRequest",
                                    "response": "JSONResponse",
                                    "options": {}
                                },
                                "LogFile": {
                                    "request": "LogFileRequest",
                                    "response": "JSONResponse",
                                    "options": {}
                                }
                            }
                        }
                    ]
                },
                {
                    "name": "status",
                    "fields": [],
                    "options": {
                        "go_package": "status"
                    },
                    "messages": [
                        {
                            "name": "StoreStatus",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "roachpb.StoreDescriptor",
                                    "name": "desc",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "map",
                                    "type": "double",
                                    "keytype": "string",
                                    "name": "metrics",
                                    "id": 2
                                }
                            ]
                        },
                        {
                            "name": "NodeStatus",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "roachpb.NodeDescriptor",
                                    "name": "desc",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "build.Info",
                                    "name": "build_info",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "started_at",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "updated_at",
                                    "id": 4,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "map",
                                    "type": "double",
                                    "keytype": "string",
                                    "name": "metrics",
                                    "id": 5
                                },
                                {
                                    "rule": "repeated",
                                    "type": "StoreStatus",
                                    "name": "store_statuses",
                                    "id": 6,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "build",
            "fields": [],
            "options": {
                "go_package": "build"
            },
            "messages": [
                {
                    "name": "Info",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "go_version",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "tag",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "time",
                            "id": 3,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "dependencies",
                            "id": 4,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "cgo_compiler",
                            "id": 5,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "string",
                            "name": "platform",
                            "id": 6,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                }
            ]
        },
        {
            "name": "gossip",
            "fields": [],
            "options": {
                "go_package": "gossip"
            },
            "messages": [
                {
                    "name": "BootstrapInfo",
                    "fields": [
                        {
                            "rule": "repeated",
                            "type": "util.UnresolvedAddr",
                            "name": "addresses",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.hlc.Timestamp",
                            "name": "timestamp",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "Request",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "node_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.customname)": "NodeID",
                                "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.UnresolvedAddr",
                            "name": "addr",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "map",
                            "type": "int64",
                            "keytype": "int32",
                            "name": "high_water_stamps",
                            "id": 3,
                            "options": {
                                "(gogoproto.castkey)": "github.com/cockroachdb/cockroach/roachpb.NodeID",
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "map",
                            "type": "Info",
                            "keytype": "string",
                            "name": "delta",
                            "id": 4
                        }
                    ]
                },
                {
                    "name": "Response",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "node_id",
                            "id": 1,
                            "options": {
                                "(gogoproto.customname)": "NodeID",
                                "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.UnresolvedAddr",
                            "name": "addr",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "util.UnresolvedAddr",
                            "name": "alternate_addr",
                            "id": 3
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "alternate_node_id",
                            "id": 4,
                            "options": {
                                "(gogoproto.customname)": "AlternateNodeID",
                                "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                            }
                        },
                        {
                            "rule": "map",
                            "type": "Info",
                            "keytype": "string",
                            "name": "delta",
                            "id": 5
                        },
                        {
                            "rule": "map",
                            "type": "int64",
                            "keytype": "int32",
                            "name": "high_water_stamps",
                            "id": 6,
                            "options": {
                                "(gogoproto.castkey)": "github.com/cockroachdb/cockroach/roachpb.NodeID",
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "InfoStatus",
                    "fields": [
                        {
                            "rule": "map",
                            "type": "Info",
                            "keytype": "string",
                            "name": "infos",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        }
                    ]
                },
                {
                    "name": "Info",
                    "fields": [
                        {
                            "rule": "optional",
                            "type": "roachpb.Value",
                            "name": "value",
                            "id": 1,
                            "options": {
                                "(gogoproto.nullable)": false
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "orig_stamp",
                            "id": 2
                        },
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "ttl_stamp",
                            "id": 3,
                            "options": {
                                "(gogoproto.customname)": "TTLStamp"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "uint32",
                            "name": "hops",
                            "id": 4
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "node_id",
                            "id": 5,
                            "options": {
                                "(gogoproto.customname)": "NodeID",
                                "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int32",
                            "name": "peer_id",
                            "id": 6,
                            "options": {
                                "(gogoproto.customname)": "PeerID",
                                "(gogoproto.casttype)": "github.com/cockroachdb/cockroach/roachpb.NodeID"
                            }
                        }
                    ]
                }
            ],
            "services": [
                {
                    "name": "Gossip",
                    "options": {},
                    "rpc": {
                        "Gossip": {
                            "request": "Request",
                            "response": "Response",
                            "options": {}
                        }
                    }
                }
            ]
        },
        {
            "name": "ts",
            "fields": [],
            "messages": [
                {
                    "name": "tspb",
                    "fields": [],
                    "options": {
                        "go_package": "tspb"
                    },
                    "messages": [
                        {
                            "name": "TimeSeriesDatapoint",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "timestamp_nanos",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "double",
                                    "name": "value",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "TimeSeriesData",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "name",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "source",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "TimeSeriesDatapoint",
                                    "name": "datapoints",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "Query",
                            "options": {
                                "(gogoproto.goproto_getters)": true
                            },
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "string",
                                    "name": "name",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "TimeSeriesQueryAggregator",
                                    "name": "downsampler",
                                    "id": 2,
                                    "options": {
                                        "default": "AVG"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "TimeSeriesQueryAggregator",
                                    "name": "source_aggregator",
                                    "id": 3,
                                    "options": {
                                        "default": "SUM"
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "TimeSeriesQueryDerivative",
                                    "name": "derivative",
                                    "id": 4,
                                    "options": {
                                        "default": "NONE"
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "string",
                                    "name": "sources",
                                    "id": 5
                                }
                            ]
                        },
                        {
                            "name": "TimeSeriesQueryRequest",
                            "fields": [
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "start_nanos",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "optional",
                                    "type": "int64",
                                    "name": "end_nanos",
                                    "id": 2,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                },
                                {
                                    "rule": "repeated",
                                    "type": "Query",
                                    "name": "queries",
                                    "id": 3,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ]
                        },
                        {
                            "name": "TimeSeriesQueryResponse",
                            "fields": [
                                {
                                    "rule": "repeated",
                                    "type": "Result",
                                    "name": "results",
                                    "id": 1,
                                    "options": {
                                        "(gogoproto.nullable)": false
                                    }
                                }
                            ],
                            "messages": [
                                {
                                    "name": "Result",
                                    "fields": [
                                        {
                                            "rule": "optional",
                                            "type": "Query",
                                            "name": "query",
                                            "id": 1,
                                            "options": {
                                                "(gogoproto.nullable)": false,
                                                "(gogoproto.embed)": true
                                            }
                                        },
                                        {
                                            "rule": "repeated",
                                            "type": "TimeSeriesDatapoint",
                                            "name": "datapoints",
                                            "id": 2,
                                            "options": {
                                                "(gogoproto.nullable)": false
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                    "enums": [
                        {
                            "name": "TimeSeriesQueryAggregator",
                            "values": [
                                {
                                    "name": "AVG",
                                    "id": 1
                                },
                                {
                                    "name": "SUM",
                                    "id": 2
                                },
                                {
                                    "name": "MAX",
                                    "id": 3
                                },
                                {
                                    "name": "MIN",
                                    "id": 4
                                }
                            ]
                        },
                        {
                            "name": "TimeSeriesQueryDerivative",
                            "values": [
                                {
                                    "name": "NONE",
                                    "id": 0
                                },
                                {
                                    "name": "DERIVATIVE",
                                    "id": 1
                                },
                                {
                                    "name": "NON_NEGATIVE_DERIVATIVE",
                                    "id": 2
                                }
                            ]
                        }
                    ],
                    "services": [
                        {
                            "name": "TimeSeries",
                            "options": {},
                            "rpc": {
                                "Query": {
                                    "request": "TimeSeriesQueryRequest",
                                    "response": "TimeSeriesQueryResponse",
                                    "options": {
                                        "(google.api.http).post": "/ts/query",
                                        "(google.api.http).body": "*"
                                    }
                                }
                            }
                        }
                    ]
                }
            ]
        }
    ]
}).build();