// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catconstants

import "github.com/lib/pq/oid"

// PgCatalogOIDsByDescID maps CockroachDB virtual table descriptor IDs for
// pg_catalog tables to their standard PostgreSQL RelationId OIDs. This is
// used when pg_dump_compatibility is enabled to return OIDs that match
// what PostgreSQL reports, allowing tools like pg_dump to correctly
// identify catalog tables.
//
// Only real PostgreSQL catalog tables are included here — pg_catalog
// entries that are views in PostgreSQL (pg_stat_activity, pg_indexes,
// pg_views, etc.) don't have stable RelationId OIDs and are excluded.
var PgCatalogOIDsByDescID = map[uint32]oid.Oid{
	PgCatalogAggregateTableID:           2600,
	PgCatalogAmTableID:                  2601,
	PgCatalogAmopTableID:                2602,
	PgCatalogAmprocTableID:              2603,
	PgCatalogAttrDefTableID:             2604,
	PgCatalogAttributeTableID:           1249,
	PgCatalogAuthIDTableID:              1260,
	PgCatalogAuthMembersTableID:         1261,
	PgCatalogCastTableID:                2605,
	PgCatalogClassTableID:               1259,
	PgCatalogCollationTableID:           3456,
	PgCatalogConstraintTableID:          2606,
	PgCatalogConversionTableID:          2607,
	PgCatalogDatabaseTableID:            1262,
	PgCatalogDbRoleSettingTableID:       2964,
	PgCatalogDefaultACLTableID:          826,
	PgCatalogDependTableID:              2608,
	PgCatalogDescriptionTableID:         2609,
	PgCatalogEnumTableID:                3501,
	PgCatalogEventTriggerTableID:        3466,
	PgCatalogExtensionTableID:           3079,
	PgCatalogForeignDataWrapperTableID:  2328,
	PgCatalogForeignServerTableID:       1417,
	PgCatalogForeignTableTableID:        3118,
	PgCatalogIndexTableID:               2610,
	PgCatalogInheritsTableID:            2611,
	PgCatalogInitPrivsTableID:           3394,
	PgCatalogLanguageTableID:            2612,
	PgCatalogLargeobjectTableID:         2613,
	PgCatalogLargeobjectMetadataTableID: 2995,
	PgCatalogNamespaceTableID:           2615,
	PgCatalogOpclassTableID:             2616,
	PgCatalogOperatorTableID:            2617,
	PgCatalogOpfamilyTableID:            2753,
	PgCatalogPartitionedTableTableID:    3350,
	PgCatalogPolicyTableID:              3256,
	PgCatalogProcTableID:                1255,
	PgCatalogPublicationTableID:         6104,
	PgCatalogPublicationRelTableID:      6106,
	PgCatalogRangeTableID:               3541,
	PgCatalogReplicationOriginTableID:   6000,
	PgCatalogRewriteTableID:             2618,
	PgCatalogSecurityLabelTableID:       3596,
	PgCatalogSequenceTableID:            2224,
	PgCatalogShdependTableID:            1214,
	PgCatalogSharedDescriptionTableID:   2396,
	PgCatalogSharedSecurityLabelTableID: 3592,
	PgCatalogStatisticTableID:           2619,
	PgCatalogStatisticExtTableID:        3381,
	PgCatalogStatisticExtDataTableID:    3429,
	PgCatalogSubscriptionTableID:        6100,
	PgCatalogSubscriptionRelTableID:     6102,
	PgCatalogTablespaceTableID:          1213,
	PgCatalogTransformTableID:           3576,
	PgCatalogTriggerTableID:             2620,
	PgCatalogTsConfigTableID:            3602,
	PgCatalogTsConfigMapTableID:         3603,
	PgCatalogTsDictTableID:              3600,
	PgCatalogTsParserTableID:            3601,
	PgCatalogTsTemplateTableID:          3764,
	PgCatalogTypeTableID:                1247,
	PgCatalogUserMappingTableID:         1418,
}

// RemapPgCatalogOid returns the standard PostgreSQL OID for a pg_catalog
// virtual table descriptor ID when remap is true. Otherwise it returns
// the descriptor ID as an OID unchanged.
func RemapPgCatalogOid(descID uint32, remap bool) oid.Oid {
	if remap {
		if pgOid, ok := PgCatalogOIDsByDescID[descID]; ok {
			return pgOid
		}
	}
	return oid.Oid(descID)
}
