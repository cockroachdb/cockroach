// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalconn

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// externalConnectionRecord is a reflective representation of a row in a
// system.external_connections table.
//
// Each field in this struct has a tag specifying the column name in the
// system.external_connections table containing the data for the field. Do not
// manipulate these fields directly, use methods in the ExternalConnection.
type externalConnectionRecord struct {
	ConnectionName    string                         `col:"connection_name"`
	ConnectionType    string                         `col:"connection_type"`
	ConnectionDetails connectionpb.ConnectionDetails `col:"connection_details"`
	Owner             username.SQLUsername           `col:"owner"`
	OwnerID           oid.Oid                        `col:"owner_id"`
}

// MutableExternalConnection is a mutable representation of an External
// Connection object.
//
// This struct can marshal/unmarshal changes made to the underlying
// system.external_connections table.
type MutableExternalConnection struct {
	// The "record" for this external connection.  Do not access this field
	// directly (except in tests); Use Get/Set methods on ExternalConnection
	// instead.
	rec externalConnectionRecord

	// Set of changes to this external connection that need to be persisted.
	dirty map[string]struct{}
}

var _ ExternalConnection = &MutableExternalConnection{}

// NewMutableExternalConnection creates and initializes a mutable
// ExternalConnection.
func NewMutableExternalConnection() *MutableExternalConnection {
	return &MutableExternalConnection{
		dirty: make(map[string]struct{}),
	}
}

// NewExternalConnection create and initializes a read-only ExternalConnection.
func NewExternalConnection(connDetails connectionpb.ConnectionDetails) ExternalConnection {
	ec := NewMutableExternalConnection()
	ec.SetConnectionType(connDetails.Type())
	ec.SetConnectionDetails(connDetails)
	return ec
}

// externalConnectionNotFoundError is returned from load when the external
// connection does not exist.
type externalConnectionNotFoundError struct {
	connectionName string
}

// Error makes scheduledJobNotFoundError an error.
func (e *externalConnectionNotFoundError) Error() string {
	return fmt.Sprintf("external connection with name %s does not exist", e.connectionName)
}

// LoadExternalConnection loads an external connection record from the
// `system.external_connections` table and returns the read-only interface for
// interacting with it.
func LoadExternalConnection(
	ctx context.Context, name string, txn isql.Txn,
) (ExternalConnection, error) {
	// Loading an External Connection is only allowed for users with the `USAGE`
	// privilege. We run the query as `node` since the user might not have
	// `SELECT` on the system table.
	row, cols, err := txn.QueryRowExWithCols(ctx, "lookup-schedule", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("SELECT * FROM system.external_connections WHERE connection_name = '%s'", name))

	if err != nil {
		return nil, errors.CombineErrors(err, &externalConnectionNotFoundError{connectionName: name})
	}
	if row == nil {
		return nil, &externalConnectionNotFoundError{connectionName: name}
	}

	ec := NewMutableExternalConnection()
	if err := ec.InitFromDatums(row, cols); err != nil {
		return nil, err
	}
	return ec, nil
}

// ConnectionName returns the connection_name.
func (e *MutableExternalConnection) ConnectionName() string {
	return e.rec.ConnectionName
}

// SetConnectionName updates the connection name.
func (e *MutableExternalConnection) SetConnectionName(name string) {
	e.rec.ConnectionName = name
	e.markDirty("connection_name")
}

// ConnectionType returns the connection_type.
func (e *MutableExternalConnection) ConnectionType() connectionpb.ConnectionType {
	return e.rec.ConnectionDetails.Type()
}

// SetConnectionType updates the connection_type.
func (e *MutableExternalConnection) SetConnectionType(connectionType connectionpb.ConnectionType) {
	e.rec.ConnectionType = connectionType.String()
	e.markDirty("connection_type")
}

// ConnectionProto returns the connection_details.
func (e *MutableExternalConnection) ConnectionProto() *connectionpb.ConnectionDetails {
	return &e.rec.ConnectionDetails
}

// SetConnectionDetails updates the connection_details.
func (e *MutableExternalConnection) SetConnectionDetails(details connectionpb.ConnectionDetails) {
	e.rec.ConnectionDetails = details
	e.markDirty("connection_details")
}

// Owner returns the owner of the External Connection object.
func (e *MutableExternalConnection) Owner() username.SQLUsername {
	return e.rec.Owner
}

// SetOwner updates the owner of the External Connection object.
func (e *MutableExternalConnection) SetOwner(owner username.SQLUsername) {
	e.rec.Owner = owner
	e.markDirty("owner")
}

// OwnerID returns the user ID of the owner of the External Connection object.
func (e *MutableExternalConnection) OwnerID() oid.Oid {
	return e.rec.OwnerID
}

// SetOwnerID updates the External Connection object's owner user ID.
func (e *MutableExternalConnection) SetOwnerID(id oid.Oid) {
	e.rec.OwnerID = id
	e.markDirty("owner_id")
}

// UnredactedConnectionStatement implements the External Connection interface.
func (e *MutableExternalConnection) UnredactedConnectionStatement() string {
	ecNode := &tree.CreateExternalConnection{
		ConnectionLabelSpec: tree.LabelSpec{
			Label: tree.NewDString(e.rec.ConnectionName),
		},
		As: tree.NewDString(e.rec.ConnectionDetails.UnredactedURI()),
	}
	return tree.AsStringWithFlags(ecNode, tree.FmtShowFullURIs)
}

// RedactedConnectionURI implements the ExternalConnection interface and
// returns the redacted URI
func (e *MutableExternalConnection) RedactedConnectionURI() string {
	unredactedURI := e.rec.ConnectionDetails.UnredactedURI()
	var err error
	switch e.rec.ConnectionType {
	case connectionpb.TypeStorage.String():
		redactedURI, err := cloud.SanitizeExternalStorageURI(unredactedURI, nil)
		if err == nil {
			return redactedURI
		}
	case connectionpb.TypeKMS.String():
		redactedURI, err := cloud.RedactKMSURI(unredactedURI)
		if err == nil {
			return redactedURI
		}
	case connectionpb.TypeForeignData.String():
		redactedURI, err := cloud.SanitizeExternalStorageURI(unredactedURI, nil)
		if err == nil {
			return redactedURI
		}
	default:
		err = fmt.Errorf("cannot redact URI for unknown connection type: %s", e.rec.ConnectionType)
	}
	return fmt.Sprintf("failed to redact the URI: %s", err.Error())
}

// datumToNative is a helper to convert tree.Datum into Go native types.  We
// only care about types stored in the system.external_connections table.
func datumToNative(datum tree.Datum) (interface{}, error) {
	datum = tree.UnwrapDOidWrapper(datum)
	if datum == tree.DNull {
		return nil, nil
	}
	switch d := datum.(type) {
	case *tree.DString:
		return string(*d), nil
	case *tree.DBytes:
		return []byte(*d), nil
	case *tree.DTimestamp:
		return d.Time, nil
	case *tree.DOid:
		return d.Oid, nil
	}
	return nil, errors.Newf("cannot handle type %T", datum)
}

// InitFromDatums initializes the receiver based on datums and column names.
func (e *MutableExternalConnection) InitFromDatums(
	datums []tree.Datum, cols []colinfo.ResultColumn,
) error {
	if len(datums) != len(cols) {
		return errors.Errorf(
			"datums length != columns length: %d != %d", len(datums), len(cols))
	}

	record := reflect.ValueOf(&e.rec).Elem()

	numInitialized := 0
	for i, col := range cols {
		native, err := datumToNative(datums[i])
		if err != nil {
			return err
		}

		if native == nil {
			continue
		}

		fieldNum, ok := columnNameToField[col.Name]
		if !ok {
			// Table contains columns we don't care about (e.g. created)
			continue
		}

		field := record.Field(fieldNum)

		if data, ok := native.([]byte); ok {
			// []byte == protocol message.
			if pb, ok := field.Addr().Interface().(protoutil.Message); ok {
				if err := protoutil.Unmarshal(data, pb); err != nil {
					return err
				}
			} else {
				return errors.Newf(
					"field %s with value of type %T is does not appear to be a protocol message",
					field.String(), field.Addr().Interface())
			}
		} else {
			// We ought to be able to assign native directly to our field.
			// But, be paranoid and double check.
			rv := reflect.ValueOf(native)
			if !rv.Type().AssignableTo(field.Type()) {
				// If this is the owner field it requires special treatment.
				ok := false
				if col.Name == "owner" {
					// The owner field has type SQLUsername, but the datum is a
					// simple string.  So we need to convert.
					var s string
					s, ok = native.(string)
					if ok {
						// Replace the value by one of the right type.
						rv = reflect.ValueOf(username.MakeSQLUsernameFromPreNormalizedString(s))
					}
				}
				if !ok {
					return errors.Newf("value of type %T cannot be assigned to %s",
						native, field.Type().String())
				}
			}
			field.Set(rv)
		}
		numInitialized++
	}

	if numInitialized == 0 {
		return errors.New("did not initialize any external connection field")
	}

	return nil
}

// generates "$1,$2,..." placeholders for the specified 'n' number of arguments.
func generatePlaceholders(n int) string {
	placeholders := strings.Builder{}
	for i := 1; i <= n; i++ {
		if i > 1 {
			placeholders.WriteByte(',')
		}
		placeholders.WriteString(fmt.Sprintf("$%d", i))
	}
	return placeholders.String()
}

// Create persists this external connection in the system.external_connections
// table.
//
// Only the values initialized in the receiver are persisted in the system
// table. If an error is returned, it is callers responsibility to handle it
// (e.g. rollback transaction).
func (e *MutableExternalConnection) Create(ctx context.Context, txn isql.Txn) error {
	cols, qargs, err := e.marshalChanges()
	if err != nil {
		return err
	}

	// CREATE EXTERNAL CONNECTION is only allowed for users with the
	// `EXTERNALCONNECTION` system privilege. We run the query as `node`
	// since the user might not have `INSERT` on the system table.
	createQuery := "INSERT INTO system.external_connections (%s) VALUES (%s) RETURNING connection_name"
	row, retCols, err := txn.QueryRowExWithCols(ctx, "ExternalConnection.Create", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(createQuery, strings.Join(cols, ","), generatePlaceholders(len(qargs))),
		qargs...,
	)
	if err != nil {
		// If we see a UniqueViolation it means that we are attempting to create an
		// External Connection with a `connection_name` that already exists.
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			var connectionName interface{}
			for i, col := range cols {
				if col == "connection_name" {
					connectionName = qargs[i]
					break
				}
			}
			return pgerror.Newf(pgcode.DuplicateObject,
				"external connection with connection name %s already exists", connectionName)
		}
		return errors.Wrapf(err, "failed to create new external connection")
	}
	if row == nil {
		return errors.New("failed to create new external connection")
	}

	return e.InitFromDatums(row, retCols)
}

// marshalChanges marshals all changes in the in-memory representation and returns
// the names of the columns and marshaled values.
func (e *MutableExternalConnection) marshalChanges() ([]string, []interface{}, error) {
	var cols []string
	var qargs []interface{}

	for col := range e.dirty {
		var arg tree.Datum
		var err error

		switch col {
		case `connection_name`:
			arg = tree.NewDString(e.rec.ConnectionName)
		case `connection_type`:
			arg = tree.NewDString(e.rec.ConnectionType)
		case `connection_details`:
			arg, err = marshalProto(&e.rec.ConnectionDetails)
		case `owner`:
			arg = tree.NewDString(e.rec.Owner.Normalized())
		case `owner_id`:
			if e.rec.OwnerID == 0 {
				continue
			}
			arg = tree.NewDOid(e.rec.OwnerID)
		default:
			return nil, nil, errors.Newf("cannot marshal column %q", col)
		}

		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, col)
		qargs = append(qargs, arg)
	}

	e.dirty = make(map[string]struct{})
	return cols, qargs, nil
}

// markDirty marks specified columns as dirty.
func (e *MutableExternalConnection) markDirty(cols ...string) {
	for _, col := range cols {
		e.dirty[col] = struct{}{}
	}
}

// marshalProto is a helper to serialize protocol message.
func marshalProto(message protoutil.Message) (tree.Datum, error) {
	data := make([]byte, message.Size())
	if _, err := message.MarshalTo(data); err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(data)), nil
}

var columnNameToField = make(map[string]int)

func init() {
	// Initialize columnNameToField map, mapping system.external_connections
	// columns to the appropriate fields in the externalConnectionRecord.
	j := reflect.TypeOf(externalConnectionRecord{})

	for f := 0; f < j.NumField(); f++ {
		field := j.Field(f)
		col := field.Tag.Get("col")
		if col != "" {
			columnNameToField[col] = f
		}
	}
}
