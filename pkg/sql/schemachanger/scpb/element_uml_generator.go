// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build generator
// +build generator

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

type dependsOn struct {
	from, to interface{}
}

var dependencies []dependsOn = []dependsOn{
	{(*scpb.Column)(nil), (*scpb.Table)(nil)},
	{(*scpb.ColumnName)(nil), (*scpb.Column)(nil)},
	{(*scpb.SequenceOwnedBy)(nil), (*scpb.Column)(nil)},
	{(*scpb.SequenceOwnedBy)(nil), (*scpb.Sequence)(nil)},
	{(*scpb.SequenceDependency)(nil), (*scpb.Column)(nil)},
	{(*scpb.SequenceDependency)(nil), (*scpb.Sequence)(nil)},
	{(*scpb.DefaultExpression)(nil), (*scpb.Column)(nil)},
	{(*scpb.Namespace)(nil), (*scpb.Table)(nil)},
	{(*scpb.Namespace)(nil), (*scpb.View)(nil)},
	{(*scpb.Namespace)(nil), (*scpb.Sequence)(nil)},
	{(*scpb.RelationDependedOnBy)(nil), (*scpb.View)(nil)},
	{(*scpb.RelationDependedOnBy)(nil), (*scpb.Table)(nil)},
	{(*scpb.DefaultExprTypeReference)(nil), (*scpb.Column)(nil)},
	{(*scpb.DefaultExprTypeReference)(nil), (*scpb.Type)(nil)},
	{(*scpb.OnUpdateExprTypeReference)(nil), (*scpb.Column)(nil)},
	{(*scpb.OnUpdateExprTypeReference)(nil), (*scpb.Type)(nil)},
	{(*scpb.ComputedExprTypeReference)(nil), (*scpb.Column)(nil)},
	{(*scpb.ComputedExprTypeReference)(nil), (*scpb.Type)(nil)},
	{(*scpb.ColumnTypeReference)(nil), (*scpb.Column)(nil)},
	{(*scpb.ColumnTypeReference)(nil), (*scpb.Type)(nil)},
	{(*scpb.ViewDependsOnType)(nil), (*scpb.View)(nil)},
	{(*scpb.ViewDependsOnType)(nil), (*scpb.Type)(nil)},
	{(*scpb.PrimaryIndex)(nil), (*scpb.Table)(nil)},
	{(*scpb.SecondaryIndex)(nil), (*scpb.Table)(nil)},
	{(*scpb.IndexName)(nil), (*scpb.PrimaryIndex)(nil)},
	{(*scpb.IndexName)(nil), (*scpb.SecondaryIndex)(nil)},
	{(*scpb.Partitioning)(nil), (*scpb.PrimaryIndex)(nil)},
	{(*scpb.Partitioning)(nil), (*scpb.SecondaryIndex)(nil)},
	{(*scpb.UniqueConstraint)(nil), (*scpb.SecondaryIndex)(nil)},
	{(*scpb.UniqueConstraint)(nil), (*scpb.Table)(nil)},
	{(*scpb.ConstraintName)(nil), (*scpb.UniqueConstraint)(nil)},
	{(*scpb.ConstraintName)(nil), (*scpb.CheckConstraint)(nil)},
	{(*scpb.ForeignKey)(nil), (*scpb.Table)(nil)},
	{(*scpb.ForeignKeyBackReference)(nil), (*scpb.Table)(nil)},
	{(*scpb.Locality)(nil), (*scpb.Table)(nil)},
	{(*scpb.Owner)(nil), (*scpb.Table)(nil)},
	{(*scpb.UserPrivileges)(nil), (*scpb.Table)(nil)},
	{(*scpb.Owner)(nil), (*scpb.View)(nil)},
	{(*scpb.UserPrivileges)(nil), (*scpb.View)(nil)},
	{(*scpb.Owner)(nil), (*scpb.Sequence)(nil)},
	{(*scpb.UserPrivileges)(nil), (*scpb.Sequence)(nil)},
	{(*scpb.DefaultPrivilege)(nil), (*scpb.Database)(nil)},
	{(*scpb.DefaultPrivilege)(nil), (*scpb.Schema)(nil)},
}

var (
	out = flag.String("out", "", "output file for generated UML")
)

func main() {
	flag.Parse()
	if err := run(*out); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		exit.WithCode(exit.FatalError())
	}
}
func run(out string) error {
	if out == "" {
		return fmt.Errorf("output required")
	}
	var buf bytes.Buffer

	buf.WriteString("@startuml\n")
	elementProtoType := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	for i := 0; i < elementProtoType.NumField(); i++ {
		fieldType := elementProtoType.Field(i).Type.Elem()
		buf.WriteString(fmt.Sprintf(
			"object %s\n\n",
			fieldType.Name()))
		for j := 0; j < fieldType.NumField(); j++ {
			arrayPrefix := " "
			if fieldType.Field(j).Type.Kind() == reflect.Slice {
				arrayPrefix = "[]"
			}
			buf.WriteString(
				fmt.Sprintf("%s : %s%s\n",
					fieldType.Name(),
					arrayPrefix,
					fieldType.Field(j).Name),
			)
		}
		fmt.Printf("\n")
	}
	// Emit the dependency arrows.
	for _, dep := range dependencies {
		fromType := reflect.TypeOf(dep.from).Elem()
		toType := reflect.TypeOf(dep.to).Elem()
		buf.WriteString(fmt.Sprintf(
			"%s <|-- %s\n", toType.Name(), fromType.Name()),
		)
	}
	buf.WriteString("@enduml\n")
	ioutil.WriteFile(out, buf.Bytes(), 0777)
	return nil
}
