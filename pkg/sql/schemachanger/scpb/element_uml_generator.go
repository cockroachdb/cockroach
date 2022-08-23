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
	"os"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

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
	var parentRelations bytes.Buffer

	getParentsFromField := func(f reflect.StructField) []string {
		if parentTag := f.Tag.Get("parent"); parentTag != "" {
			return strings.Split(parentTag, ", ")
		}
		return nil
	}

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
		buf.WriteString("\n")
		// The parent tag has a list of elements that are the parents
		// to this element. We will collect these and emit them later
		// in the PlantUML syntax.
		for _, parent := range getParentsFromField(elementProtoType.Field(i)) {
			parentRelations.WriteString(fmt.Sprintf(
				"%s <|-- %s\n", parent, fieldType.Name()))
		}
	}
	// Append all the object relationships at
	// the end.
	buf.Write(parentRelations.Bytes())
	buf.WriteString("@enduml\n")
	return os.WriteFile(out, buf.Bytes(), 0777)
}
