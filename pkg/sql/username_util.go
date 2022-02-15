package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"hash/fnv"
)

// GetUserID returns id of the user if role exists
func GetUserID(
	ctx context.Context, executor *InternalExecutor, txn *kv.Txn, role security.SQLUsername,
) (uuid.UUID, error) {

	var userID uuid.UUID
	query := `SELECT id FROM system.users WHERE username=$1`

	values, err := executor.QueryRowEx(ctx, "GetUserID", txn, sessiondata.InternalExecutorOverride{
		User: security.RootUserName(),
	},
		query, role)

	if err != nil {
		return userID, errors.Wrapf(err, "error looking up user %s", role)
	}

	if values != nil {
		if v := values[0]; v != tree.DNull {
			userID = (*(v.(*tree.DUuid))).UUID
		}
	}
	return userID, nil

}

// HashString allows to create
func HashString(str string) int {
	h := fnv.New32()
	h.Write([]byte(str))
	return int(h.Sum32())
}

// ToSQLIDs converts a NameList containing SQL input of usernames,
// normalizes the names and returns them as a list of SQLUsernames.
func ToSQLIDs(l []security.SQLUsername, ctx context.Context, executor *InternalExecutor, txn *kv.Txn) (map[security.SQLUsername]string, error) {
	targetRoles := make(map[security.SQLUsername]string)
	for _, role := range l {
		roleID, err := GetUserID(ctx, executor, txn, role)
		if err != nil {
			return nil, err
		}
		targetRoles[role] = roleID.String()
	}
	return targetRoles, nil
}
