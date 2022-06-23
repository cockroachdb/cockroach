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

// Package netrc contains functionality to work with netrc.
package netrc

import (
	"os"
	"path/filepath"

	"github.com/bgentry/go-netrc/netrc"
	"github.com/bufbuild/buf/private/pkg/app"
	"go.uber.org/multierr"
)

// Filename exposes the netrc filename based on the current operating system.
const Filename = netrcFilename

// Machine is a machine.
type Machine interface {
	// Empty for default machine.
	Name() string
	Login() string
	Password() string
	Account() string
}

// NewMachine creates a new Machine.
func NewMachine(
	name string,
	login string,
	password string,
	account string,
) Machine {
	return newMachine(name, login, password, account)
}

// GetMachineForName returns the Machine for the given name.
//
// Returns nil if no such Machine.
func GetMachineForName(envContainer app.EnvContainer, name string) (_ Machine, retErr error) {
	filePath, err := GetFilePath(envContainer)
	if err != nil {
		return nil, err
	}
	return getMachineForNameAndFilePath(name, filePath)
}

// PutMachines adds the given Machines to the configured netrc file.
func PutMachines(envContainer app.EnvContainer, machines ...Machine) error {
	filePath, err := GetFilePath(envContainer)
	if err != nil {
		return err
	}
	return putMachinesForFilePath(machines, filePath)
}

// DeleteMachineForName deletes the Machine for the given name, if set.
//
// Returns false if there was no Machine for the given name.
func DeleteMachineForName(envContainer app.EnvContainer, name string) (bool, error) {
	filePath, err := GetFilePath(envContainer)
	if err != nil {
		return false, err
	}
	return deleteMachineForFilePath(name, filePath)
}

// GetFilePath gets the netrc file path for the given environment.
func GetFilePath(envContainer app.EnvContainer) (string, error) {
	if netrcFilePath := envContainer.Env("NETRC"); netrcFilePath != "" {
		return netrcFilePath, nil
	}
	homeDirPath, err := app.HomeDirPath(envContainer)
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDirPath, netrcFilename), nil
}

func getMachineForNameAndFilePath(name string, filePath string) (_ Machine, retErr error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		retErr = multierr.Append(retErr, file.Close())
	}()
	netrc, err := netrc.Parse(file)
	if err != nil {
		return nil, err
	}
	netrcMachine := netrc.FindMachine(name)
	if netrcMachine == nil {
		return nil, nil
	}
	return newMachine(
		netrcMachine.Name,
		netrcMachine.Login,
		netrcMachine.Password,
		netrcMachine.Account,
	), nil
}

func putMachinesForFilePath(machines []Machine, filePath string) (retErr error) {
	file, err := os.Open(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// If a netrc file does not already exist, create one and continue.
		file, err = os.Create(filePath)
		if err != nil {
			return err
		}
	}
	defer func() {
		retErr = multierr.Append(retErr, file.Close())
	}()
	netrc, err := netrc.Parse(file)
	if err != nil {
		return err
	}
	for _, machine := range machines {
		if foundMachine := netrc.FindMachine(machine.Name()); foundMachine != nil {
			// If the machine already exists, remove it so that its entry is overwritten.
			netrc.RemoveMachine(machine.Name())
		}
		// Put the machine into the user's netrc.
		_ = netrc.NewMachine(
			machine.Name(),
			machine.Login(),
			machine.Password(),
			machine.Account(),
		)
	}
	bytes, err := netrc.MarshalText()
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, bytes, info.Mode())
}

func deleteMachineForFilePath(name string, filePath string) (_ bool, retErr error) {
	file, err := os.Open(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		// If a netrc file does not already exist, there's nothing to be done.
		return false, nil
	}
	defer func() {
		retErr = multierr.Append(retErr, file.Close())
	}()
	netrc, err := netrc.Parse(file)
	if err != nil {
		return false, err
	}
	if netrc.FindMachine(name) == nil {
		// Machine is not set, there is nothing to be done.
		return false, nil
	}
	netrc.RemoveMachine(name)
	bytes, err := netrc.MarshalText()
	if err != nil {
		return false, err
	}
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if err := os.WriteFile(filePath, bytes, info.Mode()); err != nil {
		return false, err
	}
	return true, nil
}
