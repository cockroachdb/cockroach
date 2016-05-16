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
            "name": "server",
            "fields": [],
            "messages": [
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
            "name": "ts",
            "fields": [],
            "options": {
                "go_package": "ts"
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
                                "(gogoproto.nullable)": false,
                                "(gogoproto.jsontag)": "timestamp_nanos,string"
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
                                "(gogoproto.nullable)": false,
                                "(gogoproto.jsontag)": "start_nanos,string"
                            }
                        },
                        {
                            "rule": "optional",
                            "type": "int64",
                            "name": "end_nanos",
                            "id": 2,
                            "options": {
                                "(gogoproto.nullable)": false,
                                "(gogoproto.jsontag)": "end_nanos,string"
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
            ]
        }
    ]
}).build();