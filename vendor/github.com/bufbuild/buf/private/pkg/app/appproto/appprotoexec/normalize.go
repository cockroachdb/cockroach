// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package appprotoexec

import (
	"errors"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

func normalizeCodeGeneratorResponse(
	response *pluginpb.CodeGeneratorResponse,
) (*pluginpb.CodeGeneratorResponse, error) {
	// we do validation after this function returns
	if response == nil || len(response.File) == 0 {
		return response, nil
	}

	prevFile := response.File[0]
	if prevFile.GetName() == "" {
		return nil, errors.New("first CodeGeneratorResponse.File had no name set")
	}
	// if we only have one file, just return so that we don't have to handle
	// edge cases in our loop
	if len(response.File) == 1 {
		return response, nil
	}

	var curFile *pluginpb.CodeGeneratorResponse_File
	newFiles := make([]*pluginpb.CodeGeneratorResponse_File, 0, len(response.File))
	for i := 1; i < len(response.File); i++ {
		curFile = response.File[i]
		if curFile.GetName() != "" {
			// if the name is non-empty, append the previous file to the slice
			newFiles = append(newFiles, prevFile)
			prevFile = curFile
		} else {
			// if the name is empty, append the content to the previous file
			// after making sure that there is no insertion point on the current file
			if curFile.GetInsertionPoint() != "" {
				return nil, errors.New("empty name on CodeGeneratorResponse.File with non-empty insertion point")
			}
			if curFile.Content != nil {
				if prevFile.Content == nil {
					prevFile.Content = curFile.Content
				} else {
					prevFile.Content = proto.String(
						prevFile.GetContent() + curFile.GetContent(),
					)
				}
			}
		}
		// if we are at the end of the loop, add the current file, as we
		// will not hit the beginning of the loop again
		if i == len(response.File)-1 {
			newFiles = append(newFiles, prevFile)
		}
	}

	return &pluginpb.CodeGeneratorResponse{
		Error:             response.Error,
		SupportedFeatures: response.SupportedFeatures,
		File:              newFiles,
	}, nil
}
