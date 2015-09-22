// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package main

import (
	"database/sql"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

const (
	fsSchema = `
CREATE DATABASE fs;

CREATE TABLE fs.namespace (
  parentID INT,
  name     STRING,
  id       INT,
  PRIMARY KEY (parentID, name)
);

CREATE TABLE fs.inode (
  id    INT PRIMARY KEY,
  inode STRING
);
`
)

// CFS implements a filesystem on top of cockroach.
type CFS struct {
	db *sql.DB
}

func (fs CFS) initSchema() error {
	_, err := fs.db.Exec(fsSchema)
	return err
}

func (fs CFS) create(parentID uint64, name, inode string) error {
	var id int64
	if err := fs.db.QueryRow(`SELECT experimental_unique_int()`).Scan(&id); err != nil {
		return err
	}
	tx, err := fs.db.Begin()
	if err != nil {
		return err
	}
	const sql = `
INSERT INTO fs.inode VALUES ($1, $2);
INSERT INTO fs.namespace VALUES ($3, $4, $1);
`
	if _, err := tx.Exec(sql, id, inode, parentID, name); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (fs CFS) lookup(parentID uint64, name string) (string, error) {
	const sql = `
SELECT inode FROM fs.inode WHERE id =
  (SELECT id FROM fs.namespace WHERE (parentID, name) = ($1, $2))
`
	var inode string
	if err := fs.db.QueryRow(`sql`, parentID, name).Scan(&inode); err != nil {
		return "", err
	}
	return inode, nil
}

func (fs CFS) list(parentID uint64) ([]string, error) {
	rows, err := fs.db.Query(`SELECT name, id FROM fs.namespace WHERE parentID = $1`, parentID)
	if err != nil {
		return nil, err
	}

	var results []string
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		results = append(results, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// TODO(pmattis): Lookup all of the inodes for all of the ids in single
	// "SELECT ... WHERE id IN" statement.
	return results, nil
}

// Root returns the filesystem's root node.
func (CFS) Root() (fs.Node, error) {
	return Node{}, nil
}

// GenerateInode returns a new inode ID.
func (CFS) GenerateInode(parentInode uint64, name string) uint64 {
	return 0
}

// Node implements the Node interface.
type Node struct{}

// Attr fills in 'a' with the node metadata.
func (Node) Attr(ctx context.Context, a *fuse.Attr) error {
	return fuse.ENOSYS
}

// Lookup looks for a node with 'name' in the receiver.
func (Node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	return nil, fuse.ENOSYS
}

// ReadDirAll returns the list of child inodes.
func (Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return nil, fuse.ENOSYS
}
