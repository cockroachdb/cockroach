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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/log"
)

var inputLayerNodes = flag.Int("input-layer-nodes", 32, "Number of nodes in the input layer.")
var hiddenLayerNodes = flag.Int("hidden-layer-nodes", 9, "Number of nodes in the hidden layer.")
var outputLayerNodes = flag.Int("output-layer-nodes", 32, "Number of nodes in the hidden layer.")
var hiddenLayers = flag.Int("hidden-layer-count", 1, "Number of hidden layers.")
var totalIterations = flag.Int("iterations", 100, "Number of iterations.")

var rng *rand.Rand
var maxInput int64
var testServer *server.TestServer

func createConnection(database bool) *sql.DB {
	url := fmt.Sprintf("https://root@%s?certs=test_certs", testServer.ServingAddr())
	if database {
		url += "&database=ann"
	}
	var db *sql.DB
	var err error
	if db, err = sql.Open("cockroach", url); err != nil {
		log.Fatal(err)
	}
	return db
}

// getNodeID returns a node id based on it's level and index into its layer.
// This way, we can store all nodes in a single table on the db.
func getNodeID(layer, num int) int {
	return (layer * 100000) + num
}

func getLayerNodeCount(layer int) int {
	if layer == 0 {
		return *inputLayerNodes
	}
	if layer == 1+*hiddenLayers {
		return *outputLayerNodes
	}
	return *hiddenLayerNodes
}

func addInput() int64 {
	newInput := rng.Int63n(maxInput)
	tempInput := newInput
	var binary bytes.Buffer
	db := createConnection(true)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < *inputLayerNodes; i++ {
		bit := tempInput & 1
		tempInput = tempInput >> 1
		if bit == 0 {
			binary.WriteString("0")
		} else {
			binary.WriteString("1")
		}
		if _, err := tx.Exec(`UPDATE nodes SET value=$1 WHERE id=$2`, float64(bit), getNodeID(0, i)); err != nil {
			log.Fatal(err)
		}
		// TODO(bram): Remove this once we have the feed forward network step working. This is only temporary to test
		// parsing the output nodes.
		if _, err := tx.Exec(`UPDATE nodes SET value=$1 WHERE id=$2`, float64(bit), getNodeID(*hiddenLayers+1, i)); err != nil {
			log.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Input: %d or %s\n", newInput, binary.String())
	return newInput
}

func getOutput() int64 {
	var result int64
	var binary bytes.Buffer
	db := createConnection(true)
	defer db.Close()
	rows, err := db.Query(`
SELECT id, value
FROM nodes
WHERE id >= $1
AND id < $2
ORDER BY id DESC
		`, getNodeID(*hiddenLayers+1, 0), getNodeID(*hiddenLayers+1, *outputLayerNodes))
	if err != nil {
		log.Fatal(err)
	}
	var length int
	for rows.Next() {
		length++
		var id int64
		var value float64
		if err := rows.Scan(&id, &value); err != nil {
			log.Fatal(err)
		}
		bit := !(value < 0.5)
		result = result << 1
		if bit {
			result = result | 1
			binary.WriteString("1")
		} else {
			binary.WriteString("0")
		}
	}
	if length != *outputLayerNodes {
		log.Fatalf("Incorrect output nodes in query. Expected %d, got %d.\n", *outputLayerNodes, length)
	}
	fmt.Printf("Output: %d or %s\n", result, binary.String())
	return result
}

func main() {
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	flag.Parse()
	security.SetReadFileFn(securitytest.Asset)
	testServer = server.StartTestServer(nil)
	defer testServer.Stop()
	db := createConnection(false)

	// This calculates the maximum sized input based on the each input and
	// output node representing a single bit.
	// TODO(bram): Refine this so that output and input are treated correctly.
	if *inputLayerNodes <= *outputLayerNodes {
		maxInput = int64(math.Sqrt(math.Exp2(float64(*inputLayerNodes))))
	} else {
		maxInput = int64(math.Sqrt(math.Exp2(float64(*outputLayerNodes))))
	}

	var widestLayer int
	for _, layer := range []int{*inputLayerNodes, *hiddenLayerNodes, *outputLayerNodes} {
		if layer > widestLayer {
			widestLayer = layer
		}
	}
	db.SetMaxOpenConns(widestLayer + 1)

	fmt.Printf("Artificial Neural Network Simulator\n")
	fmt.Printf("Input Nodes: %d\n", *inputLayerNodes)
	fmt.Printf("Hidden Layers: %d\n", *hiddenLayers)
	fmt.Printf("Hidden Nodes per Layer: %d\n", *hiddenLayerNodes)
	fmt.Printf("Output Nodes: %d\n", *outputLayerNodes)
	fmt.Printf("Total Layers: %d\n", 2+*hiddenLayers)
	fmt.Printf("Max Input: %d\n", maxInput)
	fmt.Printf("Concurrent Connections: %d\n", widestLayer+1)
	fmt.Printf("Total Iterations: %d\n", *totalIterations)
	// Create the database.
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS ann"); err != nil {
		log.Fatal(err)
	}
	db.Close()

	db = createConnection(true)
	defer db.Close()
	// Create the node and connection tables.
	if _, err := db.Exec(`
	   CREATE TABLE IF NOT EXISTS nodes (
	   	id BIGINT PRIMARY KEY,
	   	value FLOAT NOT NULL
	   )`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("TRUNCATE TABLE nodes"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`
	   CREATE TABLE IF NOT EXISTS connections (
	   	node1 BIGINT NOT NULL,
	   	node2 BIGINT NOT NULL,
	   	weight FLOAT NOT NULL,
	   	PRIMARY KEY (node1, node2)
	   )`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("TRUNCATE TABLE connections"); err != nil {
		log.Fatal(err)
	}

	// Populate the nodes with random values.
	for i := 0; i < *inputLayerNodes; i++ {
		if _, err := db.Exec(`INSERT INTO nodes (id, value) VALUES ($1, $2)`, getNodeID(0, i), float64(0)); err != nil {
			log.Fatal(err)
		}
	}
	for i := 0; i < *hiddenLayers; i++ {
		for j := 0; j < *hiddenLayerNodes; j++ {
			if _, err := db.Exec(`INSERT INTO nodes (id, value) VALUES ($1, $2)`, getNodeID(i+1, j), float64(0)); err != nil {
				log.Fatal(err)
			}
		}
	}
	for i := 0; i < *outputLayerNodes; i++ {
		if _, err := db.Exec(`INSERT INTO nodes (id, value) VALUES ($1, $2)`, getNodeID(1+*hiddenLayers, i), float64(0)); err != nil {
			log.Fatal(err)
		}
	}

	// Populate the connections with random weights.
	for i := 1; i < *hiddenLayers+2; i++ {
		for j := 0; j < getLayerNodeCount(i); j++ {
			for k := 0; k < getLayerNodeCount(i-1); k++ {
				if _, err := db.Exec(`INSERT INTO connections (node1, node2, weight) VALUES ($1, $2, $3)`, getNodeID(i-1, k), getNodeID(i, j), rng.Float64()); err != nil {
					log.Fatal(err)
				}
			}
		}
	}

	// Run the iterations.
	for i := 0; i < *totalIterations; i++ {
		fmt.Printf("------ Iteration %d -------\n", i)
		addInput()
		// TODO(bram): Add feed forward propagation here.
		getOutput()
		// TODO(bram): Add training of weights here.
	}
}
