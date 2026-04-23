package main

import (
	"context"
	"fmt"
	"strings"
)

// isAlreadyExists checks if a database error indicates the object already exists.
func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

type book struct {
	Title   string
	Content string
	Price   float64
	Regions []string
}

var demoBooks = []book{
	{
		Title: "Designing Data-Intensive Applications",
		Content: `A comprehensive guide to the principles and practicalities of data systems
design. The book covers the foundations of data systems including reliability,
scalability, and maintainability. It explores data models and query languages,
comparing relational, document, and graph databases.

The book dives deep into storage engines, examining how databases store and retrieve
data using B-trees and LSM-trees. It explains the trade-offs between different
indexing structures and their impact on read and write performance. Replication
strategies are covered in detail, including single-leader, multi-leader, and
leaderless approaches, along with consistency guarantees.

Partitioning (sharding) strategies are discussed, showing how to distribute data
across multiple machines for scalability. The book also covers transactions, including
ACID properties, isolation levels, and the challenges of distributed transactions.
Batch and stream processing architectures are explained, including MapReduce, Spark,
and event sourcing patterns.`,
		Price:   45.99,
		Regions: []string{"us-east", "us-west", "eu-west", "ap-south"},
	},
	{
		Title: "The Pragmatic Programmer",
		Content: `A guide to software craftsmanship that has shaped how developers think about
their work. The book emphasizes taking responsibility for your code and career,
encouraging developers to be pragmatic rather than dogmatic about technology choices.

Key principles include DRY (Don't Repeat Yourself), the importance of orthogonality
in system design, and the value of tracer bullets for quickly getting feedback on
architectural decisions. The book advocates for learning a new programming language
every year and mastering your tools, especially text editors and shell commands.

The authors discuss estimation techniques, prototyping, and the power of plain text
for data storage. They introduce the concept of programming by coincidence versus
programming deliberately, and stress the importance of testing including unit tests,
integration tests, and property-based testing. Design by contract and assertive
programming are presented as defensive coding strategies.

The book concludes with advice on team dynamics, communication, and the importance
of continuously improving your skills. It remains one of the most recommended books
for developers at all experience levels.`,
		Price:   42.50,
		Regions: []string{"us-east", "us-west", "eu-west", "eu-central"},
	},
	{
		Title: "Clean Code",
		Content: `A handbook of agile software craftsmanship focused on writing readable,
maintainable code. The book argues that code quality is not just about making things
work, but about making code that other developers can understand and modify.

The author presents rules for naming variables, functions, and classes — names should
be intention-revealing, pronounceable, and searchable. Functions should be small, do
one thing, and operate at a single level of abstraction. Comments should be avoided
when possible; instead, write self-documenting code.

Error handling is covered extensively, advocating for exceptions over return codes
and emphasizing that error handling should not obscure the main logic. The book
discusses object-oriented design principles, including the Single Responsibility
Principle and the Open-Closed Principle.

Testing is a central theme — the book introduces the FIRST principles for tests
(Fast, Independent, Repeatable, Self-Validating, Timely). Test-driven development
is encouraged, with the three laws of TDD. The book also covers code smells and
refactoring techniques for identifying and fixing problematic patterns.`,
		Price:   38.99,
		Regions: []string{"us-east", "eu-west", "eu-central", "ap-south"},
	},
	{
		Title: "Introduction to Algorithms",
		Content: `The definitive textbook on algorithms, commonly known as CLRS. It provides a
comprehensive introduction to the modern study of computer algorithms, covering a
broad range of topics in depth while remaining accessible to readers at all levels.

The book begins with foundational concepts: asymptotic notation, recurrences, and
the master theorem for analyzing algorithm complexity. Sorting algorithms are covered
thoroughly — insertion sort, merge sort, heapsort, quicksort, and linear-time sorting
algorithms like counting sort and radix sort.

Data structures form a major part of the book, including hash tables, binary search
trees, red-black trees, B-trees, and Fibonacci heaps. Graph algorithms are covered
extensively: breadth-first search, depth-first search, topological sort, minimum
spanning trees (Kruskal's and Prim's algorithms), and shortest paths (Dijkstra's
and Bellman-Ford).

Advanced topics include dynamic programming, greedy algorithms, amortized analysis,
network flows, and NP-completeness theory. The fourth edition adds new chapters on
matchings in bipartite graphs, online algorithms, and machine learning algorithms.`,
		Price:   89.99,
		Regions: []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"},
	},
	{
		Title: "The Go Programming Language",
		Content: `The authoritative resource for learning Go, written by Alan Donovan and Brian
Kernighan. The book covers the language from basics to advanced topics, with a focus
on writing clear, idiomatic Go code.

It begins with program structure, data types, and composite types (arrays, slices,
maps, structs). Functions are covered in depth, including variadic functions, deferred
function calls, and error handling patterns. The book explains Go's approach to
object-oriented programming through methods and interfaces, emphasizing composition
over inheritance.

Goroutines and channels form the core of Go's concurrency model. The book explains
how to use goroutines for concurrent execution, channels for communication, and the
select statement for multiplexing. Common concurrency patterns are demonstrated,
including pipelines, fan-out/fan-in, and cancellation with context.

The book also covers the standard library extensively, including I/O, HTTP servers
and clients, JSON encoding, and the testing package. Low-level programming topics
like unsafe pointers and cgo are addressed. Reflection and code generation round
out the advanced topics.`,
		Price:   34.99,
		Regions: []string{"us-east", "us-west", "eu-west"},
	},
	{
		Title: "Database Internals",
		Content: `A deep dive into how distributed databases work under the hood. The book is
divided into two parts: storage engines and distributed systems.

The storage engine section covers B-tree variants (B+ trees, B* trees, copy-on-write
B-trees), LSM-trees, and their trade-offs. It explains write-ahead logging, buffer
management, and how storage engines handle concurrency through lock-based and
optimistic protocols. Page layout, slotted pages, and cell formats are discussed
in detail.

The distributed systems section covers failure detection using heartbeats and phi-accrual
detectors, leader election algorithms (Bully, Raft), and distributed consensus
protocols including Paxos, Multi-Paxos, and Raft. Replication strategies — both
synchronous and asynchronous — are analyzed with their consistency implications.

Anti-entropy protocols like Merkle trees and gossip-based dissemination are explained.
The book covers distributed transactions, including two-phase commit, three-phase
commit, and Calvin-style deterministic transactions. Consistency models from
linearizability to eventual consistency are compared with practical examples.`,
		Price:   52.99,
		Regions: []string{"us-east", "us-west", "eu-west", "eu-central"},
	},
	{
		Title: "Grokking Algorithms",
		Content: `An illustrated guide to algorithms for beginners. The book uses hundreds of
pictures and real-world examples to make algorithms approachable and fun to learn.

It starts with binary search and Big O notation, explaining how to reason about
algorithm performance. Selection sort is introduced as the first sorting algorithm,
followed by recursion and the call stack. Quicksort is used to teach divide-and-conquer
strategies and average-case versus worst-case analysis.

Hash tables are presented as one of the most useful data structures, with practical
examples like building a DNS cache or a voting system. Breadth-first search introduces
graph algorithms, showing how to find the shortest path in unweighted graphs.
Dijkstra's algorithm extends this to weighted graphs.

The book covers greedy algorithms with the set-covering problem, dynamic programming
with the knapsack problem and longest common subsequence, and nearest-neighbor
algorithms for approximating solutions to NP-hard problems. The final chapter
introduces more advanced topics like MapReduce, bloom filters, and machine learning
basics.`,
		Price:   29.99,
		Regions: []string{"us-east", "us-west", "eu-west", "ap-south", "ap-east"},
	},
	{
		Title: "Site Reliability Engineering",
		Content: `How Google runs production systems, covering the principles and practices of
site reliability engineering (SRE). The book defines SRE as what happens when you
treat operations as a software engineering problem.

Service level objectives (SLOs), service level indicators (SLIs), and error budgets
form the foundation of the SRE approach. Teams set reliability targets and use error
budgets to balance the velocity of feature development against system stability.
When the error budget is exhausted, development slows to focus on reliability.

Monitoring and alerting are critical SRE practices. The book advocates for
symptom-based alerting over cause-based alerting, and introduces the four golden
signals: latency, traffic, errors, and saturation. On-call rotation practices,
incident management, and blameless postmortems are covered in detail.

The book also discusses capacity planning, load balancing, and handling overload
through techniques like load shedding and graceful degradation. Release engineering
practices including canary deployments, feature flags, and progressive rollouts are
explained. The toil budget concept is introduced — SRE teams should spend no more
than 50% of their time on operational work.`,
		Price:   55.00,
		Regions: []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south"},
	},
	{
		Title: "Hands-On Machine Learning",
		Content: `A practical guide to machine learning using Scikit-Learn, Keras, and
TensorFlow. The book takes a hands-on approach, building real-world projects from
start to finish.

Part one covers classical machine learning: linear regression, logistic regression,
support vector machines, decision trees, random forests, and ensemble methods. The
book walks through the full ML pipeline — data collection, cleaning, feature
engineering, model selection, training, and evaluation. Cross-validation, grid search,
and regularization techniques are explained with practical examples.

Part two focuses on deep learning with neural networks. It covers dense networks,
convolutional neural networks (CNNs) for image recognition, and recurrent neural
networks (RNNs) for sequence processing. The attention mechanism and transformer
architecture are explained, showing how they revolutionized natural language processing.

Transfer learning, generative adversarial networks (GANs), and reinforcement learning
are covered in the advanced chapters. The book includes practical advice on deploying
models to production, handling data drift, and scaling training across multiple GPUs.
Each chapter includes exercises with solutions available online.`,
		Price:   59.99,
		Regions: []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"},
	},
	{
		Title: "The Art of PostgreSQL",
		Content: `A comprehensive guide to mastering SQL through PostgreSQL. The book argues
that SQL is a powerful programming language in its own right and that many application
problems can be solved more elegantly in SQL than in application code.

The book covers window functions in depth — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD,
and NTILE — showing how they simplify complex analytical queries. Common table
expressions (CTEs) are presented for writing readable, composable queries, including
recursive CTEs for hierarchical data traversal.

Advanced indexing strategies are covered: B-tree indexes, GIN indexes for full-text
search and JSONB, GiST indexes for geometric and range types, and partial indexes
for selective optimization. The book explains EXPLAIN ANALYZE output and how to
read query plans to identify performance bottlenecks.

JSON and JSONB support in PostgreSQL is explored for semi-structured data. The book
discusses lateral joins, array operations, and the generate_series function for data
generation. Triggers, stored procedures, and event-driven architectures within
PostgreSQL are covered. Partitioning strategies for large tables and the LISTEN/NOTIFY
mechanism for real-time notifications complete the advanced topics.`,
		Price:   49.99,
		Regions: []string{"us-east", "eu-west", "eu-central"},
	},
}

// SetupDemo creates the books table, inserts sample data, and creates a
// vectorizer to auto-embed all rows.
func (s *ChatService) SetupDemo(ctx context.Context) error {
	fmt.Println("Setting up demo dataset...")

	// Enable vectorize feature.
	if _, err := s.db.ExecContext(ctx,
		`SET CLUSTER SETTING sql.vectorize.enabled = true`,
	); err != nil {
		return fmt.Errorf("enabling vectorize: %w", err)
	}

	// Create books table.
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS books (
			id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
			title STRING NOT NULL,
			content TEXT NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			region STRING[] NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	fmt.Println("  Created books table")

	// Insert demo books.
	for _, b := range demoBooks {
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO books (title, content, price, region) VALUES ($1, $2, $3, $4)`,
			b.Title, b.Content, b.Price, fmt.Sprintf("{%s}", strings.Join(b.Regions, ",")),
		); err != nil {
			return fmt.Errorf("inserting %q: %w", b.Title, err)
		}
	}
	fmt.Printf("  Inserted %d books\n", len(demoBooks))

	// Create vectorizer on title + content columns.
	// Schedule every 1 minute for quick demo turnaround.
	if _, err := s.db.ExecContext(ctx, `
		CREATE VECTORIZER ON books
		USING COLUMN (title, content)
		WITH schedule = '@every 1m'
	`); err != nil {
		if !isAlreadyExists(err) {
			return fmt.Errorf("creating vectorizer: %w", err)
		}
		fmt.Println("  Vectorizer already exists, skipping")
	} else {
		fmt.Println("  Created vectorizer on books(title, content)")
	}

	fmt.Println()
	fmt.Println("Setup complete! The vectorizer will embed all rows in the background.")
	fmt.Println("Check progress with:")
	fmt.Println("  SELECT count(*) FROM books_embeddings;")
	fmt.Println()
	fmt.Println("Once embeddings are populated, run without --setup to start chatting.")

	return nil
}
