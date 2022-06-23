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

package protosource

import (
	"sync"

	"google.golang.org/protobuf/types/descriptorpb"
)

type locationStore struct {
	sourceCodeInfoLocations []*descriptorpb.SourceCodeInfo_Location

	pathToLocation               map[string]Location
	pathToSourceCodeInfoLocation map[string]*descriptorpb.SourceCodeInfo_Location
	locationLock                 sync.RWMutex
	sourceCodeInfoLocationLock   sync.RWMutex
}

func newLocationStore(sourceCodeInfoLocations []*descriptorpb.SourceCodeInfo_Location) *locationStore {
	return &locationStore{
		sourceCodeInfoLocations: sourceCodeInfoLocations,
		pathToLocation:          make(map[string]Location),
	}
}

func (l *locationStore) getLocation(path []int32) Location {
	return l.getLocationByPathKey(getPathKey(path))
}

// optimization for keys we know ahead of time such as package location, certain file options
func (l *locationStore) getLocationByPathKey(pathKey string) Location {
	// check cache first
	l.locationLock.RLock()
	location, ok := l.pathToLocation[pathKey]
	l.locationLock.RUnlock()
	if ok {
		return location
	}

	// build index and get sourceCodeInfoLocation
	l.sourceCodeInfoLocationLock.RLock()
	pathToSourceCodeInfoLocation := l.pathToSourceCodeInfoLocation
	l.sourceCodeInfoLocationLock.RUnlock()
	if pathToSourceCodeInfoLocation == nil {
		l.sourceCodeInfoLocationLock.Lock()
		pathToSourceCodeInfoLocation = l.pathToSourceCodeInfoLocation
		if pathToSourceCodeInfoLocation == nil {
			pathToSourceCodeInfoLocation = make(map[string]*descriptorpb.SourceCodeInfo_Location)
			for _, sourceCodeInfoLocation := range l.sourceCodeInfoLocations {
				pathKey := getPathKey(sourceCodeInfoLocation.Path)
				// - Multiple locations may have the same path.  This happens when a single
				//   logical declaration is spread out across multiple places.  The most
				//   obvious example is the "extend" block again -- there may be multiple
				//   extend blocks in the same scope, each of which will have the same path.
				if _, ok := pathToSourceCodeInfoLocation[pathKey]; !ok {
					pathToSourceCodeInfoLocation[pathKey] = sourceCodeInfoLocation
				}
			}
		}
		l.pathToSourceCodeInfoLocation = pathToSourceCodeInfoLocation
		l.sourceCodeInfoLocationLock.Unlock()
	}
	sourceCodeInfoLocation, ok := pathToSourceCodeInfoLocation[pathKey]
	if !ok {
		return nil
	}

	// populate cache and return
	if sourceCodeInfoLocation == nil {
		location = nil
	} else {
		location = newLocation(sourceCodeInfoLocation)
	}
	l.locationLock.Lock()
	l.pathToLocation[pathKey] = location
	l.locationLock.Unlock()
	return location
}

func getPathKey(path []int32) string {
	key := make([]byte, len(path)*4)
	j := 0
	for _, elem := range path {
		key[j] = byte(elem)
		key[j+1] = byte(elem >> 8)
		key[j+2] = byte(elem >> 16)
		key[j+3] = byte(elem >> 24)
		j += 4
	}
	return string(key)
}
