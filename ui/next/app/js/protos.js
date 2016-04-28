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
            "options": {
                "go_package": "server"
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
            ],
            "enums": [
                {
                    "name": "DrainMode",
                    "values": [
                        {
                            "name": "CLIENT",
                            "id": 0
                        },
                        {
                            "name": "LEADERSHIP",
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