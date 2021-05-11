// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aws

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const sshPublicKeyFile = "${HOME}/.ssh/id_rsa.pub"

// sshKeyExists checks to see if there is a an SSH key with the given name in the given region.
func (p *Provider) sshKeyExists(keyName string, region string) (bool, error) {
	var data struct {
		KeyPairs []struct {
			KeyName string
		}
	}
	args := []string{
		"ec2", "describe-key-pairs",
		"--region", region,
	}
	err := p.runJSONCommand(args, &data)
	if err != nil {
		return false, err
	}
	for _, keyPair := range data.KeyPairs {
		if keyPair.KeyName == keyName {
			return true, nil
		}
	}
	return false, nil
}

// sshKeyImport takes the user's local, public SSH key and imports it into the ec2 region so that
// we can create new hosts with it.
func (p *Provider) sshKeyImport(keyName string, region string) error {
	_, err := os.Stat(os.ExpandEnv(sshPublicKeyFile))
	if err != nil {
		if oserror.IsNotExist(err) {
			return errors.Wrapf(err, "please run ssh-keygen externally to create your %s file", sshPublicKeyFile)
		}
		return err
	}

	var data struct {
		KeyName string
	}
	_ = data.KeyName // silence unused warning
	args := []string{
		"ec2", "import-key-pair",
		"--region", region,
		"--key-name", keyName,
		"--public-key-material", fmt.Sprintf("fileb://%s", sshPublicKeyFile),
	}
	err = p.runJSONCommand(args, &data)
	// If two roachprod instances run at the same time with the same key, they may
	// race to upload the key pair.
	if err == nil || strings.Contains(err.Error(), "InvalidKeyPair.Duplicate") {
		return nil
	}
	return err
}

// sshKeyName computes the name of the ec2 ssh key that we'll store the local user's public key in
func (p *Provider) sshKeyName() (string, error) {
	user, err := p.FindActiveAccount()
	if err != nil {
		return "", err
	}

	keyBytes, err := ioutil.ReadFile(os.ExpandEnv(sshPublicKeyFile))
	if err != nil {
		if oserror.IsNotExist(err) {
			return "", errors.Wrapf(err, "please run ssh-keygen externally to create your %s file", sshPublicKeyFile)
		}
		return "", err
	}

	hash := sha1.New()
	if _, err := hash.Write(keyBytes); err != nil {
		return "", err
	}
	hashBytes := hash.Sum(nil)
	hashText := base64.URLEncoding.EncodeToString(hashBytes)

	return fmt.Sprintf("%s-%s", user, hashText), nil
}
