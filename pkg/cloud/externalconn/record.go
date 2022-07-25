// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package externalconn

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
}

// ExternalConnection is a representation of an External Connection.
//
// This struct can marshal/unmarshal changes made to the underlying
// system.external_connections table.
type ExternalConnection struct {
	// The "record" for this external connection.  Do not access this field
	// directly (except in tests); Use Get/Set methods on ExternalConnection
	// instead.
	rec externalConnectionRecord

	// Set of changes to this external connection that need to be persisted.
	dirty map[string]struct{}
}

// NewExternalConnection creates and initializes an ExternalConnection.
func NewExternalConnection() *ExternalConnection {
	return &ExternalConnection{
		dirty: make(map[string]struct{}),
	}
}

// SetConnectionName updates the connection name.
func (e *ExternalConnection) SetConnectionName(name string) {
	e.rec.ConnectionName = name
	e.markDirty("connection_name")
}

// SetConnectionType updates the connection_type.
func (e *ExternalConnection) SetConnectionType(connectionType string) {
	e.rec.ConnectionType = connectionType
	e.markDirty("connection_type")
}

// SetConnectionDetails updates the connection_details.
func (e *ExternalConnection) SetConnectionDetails(details connectionpb.ConnectionDetails) {
	e.rec.ConnectionDetails = details
	e.markDirty("connection_details")
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
	}
	return nil, errors.Newf("cannot handle type %T", datum)
}

// InitFromDatums initializes the receiver based on datums and column names.
func (e *ExternalConnection) InitFromDatums(
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
				return errors.Newf("value of type %T cannot be assigned to %s",
					native, field.Type().String())
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
func (e *ExternalConnection) Create(
	ctx context.Context, ex sqlutil.InternalExecutor, user username.SQLUsername, txn *kv.Txn,
) error {
	cols, qargs, err := e.marshalChanges()
	if err != nil {
		return err
	}

	// CREATE EXTERNAL CONNECTION is only allowed for users with the
	// `EXTERNALCONNECTION` system privilege. We run the query as `node`
	// since the user might not have `INSERT` on the system table.
	createQuery := "INSERT INTO system.external_connections (%s) VALUES (%s) RETURNING connection_name"
	row, retCols, err := ex.QueryRowExWithCols(ctx, "ExternalConnection.Create", txn,
		sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
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
func (e *ExternalConnection) marshalChanges() ([]string, []interface{}, error) {
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
func (e *ExternalConnection) markDirty(cols ...string) {
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
