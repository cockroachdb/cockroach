// This is a customized version of https://github.com/gogo/protobuf/blob/master/protoc-gen-gogoslick/main.go

package main

import (
	"github.com/gogo/protobuf/vanity"
	"github.com/gogo/protobuf/vanity/command"
)

func main() {
	req := command.Read()
	files := req.GetProtoFile()

	vanity.ForEachFile(files, vanity.TurnOnMarshalerAll)
	vanity.ForEachFile(files, vanity.TurnOnSizerAll)
	vanity.ForEachFile(files, vanity.TurnOnUnmarshalerAll)

	// either one of these breaks compilation of the descriptor proto :(
	// vanity.ForEachFieldInFiles(files, vanity.TurnOffNullableForNativeTypesOnly)
	// vanity.ForEachFile(files, vanity.TurnOffGoGettersAll)

	// do we want to support unknown fields?
	vanity.ForEachFile(files, vanity.TurnOffGoUnrecognizedAll)

	// prefixes usually make sense
	// vanity.ForEachFile(files, vanity.TurnOffGoEnumPrefixAll)

	vanity.ForEachFile(files, vanity.TurnOffGoEnumStringerAll)
	vanity.ForEachFile(files, vanity.TurnOnEnumStringerAll)

	// generates broken code
	// vanity.ForEachFile(files, vanity.TurnOnEqualAll)

	// we have some custom stringers
	// vanity.ForEachFile(files, vanity.TurnOnGoStringAll)
	// vanity.ForEachFile(files, vanity.TurnOffGoStringerAll)
	// vanity.ForEachFile(files, vanity.TurnOnStringerAll)

	resp := command.Generate(req)
	command.Write(resp)
}
