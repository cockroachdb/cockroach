// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package privilege

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Kind defines a privilege. This is output by the parser, and used to
// generate the privilege bitfields in the PrivilegeDescriptor.
type Kind uint32

// List of privileges. ALL is specifically encoded so that it will automatically
// pick up new privileges.
// Do not change values of privileges. These correspond to the position
// of the privilege in a bit field and are expected to stay constant.
const (
	ALL    Kind = 1
	CREATE Kind = 2
	DROP   Kind = 3
	// This is a placeholder to make sure that 4 is not reused.
	//
	// It was previously used for the GRANT privilege that has been replaced
	// with the more granular Privilege.GrantOption.
	_                        Kind = 4
	SELECT                   Kind = 5
	INSERT                   Kind = 6
	DELETE                   Kind = 7
	UPDATE                   Kind = 8
	USAGE                    Kind = 9
	ZONECONFIG               Kind = 10
	CONNECT                  Kind = 11
	RULE                     Kind = 12
	MODIFYCLUSTERSETTING     Kind = 13
	EXTERNALCONNECTION       Kind = 14
	VIEWACTIVITY             Kind = 15
	VIEWACTIVITYREDACTED     Kind = 16
	VIEWCLUSTERSETTING       Kind = 17
	CANCELQUERY              Kind = 18
	NOSQLLOGIN               Kind = 19
	EXECUTE                  Kind = 20
	VIEWCLUSTERMETADATA      Kind = 21
	VIEWDEBUG                Kind = 22
	BACKUP                   Kind = 23
	RESTORE                  Kind = 24
	EXTERNALIOIMPLICITACCESS Kind = 25
	CHANGEFEED               Kind = 26
	VIEWJOB                  Kind = 27
	MODIFYSQLCLUSTERSETTING  Kind = 28
	REPLICATION              Kind = 29
	MANAGEVIRTUALCLUSTER     Kind = 30
	VIEWSYSTEMTABLE          Kind = 31
	CREATEROLE               Kind = 32
	CREATELOGIN              Kind = 33
	CREATEDB                 Kind = 34
	CONTROLJOB               Kind = 35
	REPAIRCLUSTER            Kind = 36
	TRIGGER                  Kind = 37
	BYPASSRLS                Kind = 38
	largestKind                   = BYPASSRLS
)

var isDeprecatedKind = map[Kind]bool{
	4: true,
}

// KindInternalKey is the value stored in system tables, etc, that represent the
// privilege internally. It is not visible to end-users.
type KindInternalKey string

// InternalKey returns the KindInternalKey for a Kind (see docs on
// KindInternalKey). The InternalKey must not change between releases.
func (k Kind) InternalKey() KindInternalKey {
	switch k {
	case ALL:
		return "ALL"
	case CREATE:
		return "CREATE"
	case DROP:
		return "DROP"
	case SELECT:
		return "SELECT"
	case INSERT:
		return "INSERT"
	case DELETE:
		return "DELETE"
	case UPDATE:
		return "UPDATE"
	case USAGE:
		return "USAGE"
	case ZONECONFIG:
		return "ZONECONFIG"
	case CONNECT:
		return "CONNECT"
	case RULE:
		return "RULE"
	case MODIFYCLUSTERSETTING:
		return "MODIFYCLUSTERSETTING"
	case EXTERNALCONNECTION:
		return "EXTERNALCONNECTION"
	case VIEWACTIVITY:
		return "VIEWACTIVITY"
	case VIEWACTIVITYREDACTED:
		return "VIEWACTIVITYREDACTED"
	case VIEWCLUSTERSETTING:
		return "VIEWCLUSTERSETTING"
	case CANCELQUERY:
		return "CANCELQUERY"
	case NOSQLLOGIN:
		return "NOSQLLOGIN"
	case EXECUTE:
		return "EXECUTE"
	case VIEWCLUSTERMETADATA:
		return "VIEWCLUSTERMETADATA"
	case VIEWDEBUG:
		return "VIEWDEBUG"
	case BACKUP:
		return "BACKUP"
	case RESTORE:
		return "RESTORE"
	case EXTERNALIOIMPLICITACCESS:
		return "EXTERNALIOIMPLICITACCESS"
	case CHANGEFEED:
		return "CHANGEFEED"
	case VIEWJOB:
		return "VIEWJOB"
	case MODIFYSQLCLUSTERSETTING:
		return "MODIFYSQLCLUSTERSETTING"
	case REPLICATION:
		return "REPLICATION"
	case MANAGEVIRTUALCLUSTER:
		// This Kind was renamed during 23.2 cycle.
		return "MANAGETENANT"
	case VIEWSYSTEMTABLE:
		return "VIEWSYSTEMTABLE"
	case CREATEROLE:
		return "CREATEROLE"
	case CREATELOGIN:
		return "CREATELOGIN"
	case CREATEDB:
		return "CREATEDB"
	case CONTROLJOB:
		return "CONTROLJOB"
	case REPAIRCLUSTER:
		return "REPAIRCLUSTERMETADATA"
	case TRIGGER:
		return "TRIGGER"
	case BYPASSRLS:
		return "BYPASSRLS"
	default:
		panic(errors.AssertionFailedf("unhandled kind: %d", int(k)))
	}
}

// KindDisplayName is the string representation of privileges
// displayed to end users and recognized by the parser. The name can
// be different from the key (e.g. when we choose a better name).
type KindDisplayName string

// DisplayName reports the display name for a privilege.
func (k Kind) DisplayName() KindDisplayName {
	switch k {
	case MANAGEVIRTUALCLUSTER:
		return "MANAGEVIRTUALCLUSTER"
	case REPAIRCLUSTER:
		return "REPAIRCLUSTER"
	default:
		// Unless we have an exception above, the internal
		// key is also a valid display name.
		return KindDisplayName(k.InternalKey())
	}
}

// SafeValue implements the redact.SafeValuer interface.
func (KindDisplayName) SafeValue() {}

// SafeValue implements the redact.SafeValuer interface.
func (KindInternalKey) SafeValue() {}

// SafeFormat implements the redact.SafeFormatter interface.
func (k Kind) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(k.DisplayName())
}

// KeyToName converts a privilege key to its name.
func KeyToName(key string) (string, error) {
	kind, ok := ByInternalKey[KindInternalKey(strings.ToUpper(key))]
	if !ok {
		return "", errors.Errorf("not a valid privilege: %q", key)
	}
	return string(kind.DisplayName()), nil
}

// ByDisplayName is a map of display name -> kind value. It is populated by
// init. All names use upper case.
//
// Note that all internal keys are also added into this map to allow for
// backward-compatibility.
var ByDisplayName map[KindDisplayName]Kind

// ByInternalKey is a map of internal key -> kind value. It is populated by
// init. All keys use upper case.
var ByInternalKey map[KindInternalKey]Kind

func init() {
	AllPrivileges = make([]Kind, 0, largestKind)
	ByDisplayName = make(map[KindDisplayName]Kind)
	ByInternalKey = make(map[KindInternalKey]Kind)

	for kind := ALL; kind <= largestKind; kind++ {
		if isDeprecatedKind[kind] {
			continue
		}
		AllPrivileges = append(AllPrivileges, kind)
		ByInternalKey[kind.InternalKey()] = kind

		ByDisplayName[kind.DisplayName()] = kind
		// It should also be possible to look up a privilege using its internal
		// key.
		ByDisplayName[KindDisplayName(kind.InternalKey())] = kind
	}
}
