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

// SetupAll runs both the data load and vectorizer creation steps.
func (s *ChatService) SetupAll(ctx context.Context) error {
	if err := s.SetupLoad(ctx); err != nil {
		return err
	}
	return s.SetupVectorizer(ctx)
}

// SetupLoad creates the papers table and fetches CS research papers from arXiv.
func (s *ChatService) SetupLoad(ctx context.Context) error {
	fmt.Println("Loading CS research paper archive...")

	// Create papers table.
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS papers (
			id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
			arxiv_id STRING NOT NULL UNIQUE,
			title STRING NOT NULL,
			authors STRING NOT NULL,
			abstract TEXT NOT NULL,
			category STRING NOT NULL,
			pdf_link STRING NOT NULL,
			published TIMESTAMP NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	fmt.Println("  Created papers table")

	// Fetch papers from arXiv across 10 CS categories.
	fmt.Println("  Fetching papers from arXiv (this may take several minutes)...")
	papers, err := fetchAllPapers(ctx)
	if err != nil {
		return fmt.Errorf("fetching papers: %w", err)
	}
	fmt.Printf("  Fetched %d unique papers\n", len(papers))

	// Insert papers in batches using multi-row INSERT.
	inserted := 0
	const insertBatchSize = 100
	for i := 0; i < len(papers); i += insertBatchSize {
		end := i + insertBatchSize
		if end > len(papers) {
			end = len(papers)
		}
		batch := papers[i:end]

		var valueStrings []string
		var args []interface{}
		for j, p := range batch {
			base := j * 7
			valueStrings = append(valueStrings,
				fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
					base+1, base+2, base+3, base+4, base+5, base+6, base+7))
			args = append(args,
				p.ArxivID, p.Title, p.Authors, p.Abstract,
				p.Category, p.PDFLink, p.Published)
		}

		query := fmt.Sprintf(`
			INSERT INTO papers (arxiv_id, title, authors, abstract, category, pdf_link, published)
			VALUES %s
			ON CONFLICT (arxiv_id) DO NOTHING
		`, strings.Join(valueStrings, ", "))

		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("inserting batch at offset %d: %w", i, err)
		}
		inserted += len(batch)
		if inserted%500 == 0 || inserted == len(papers) {
			fmt.Printf("  Inserted %d/%d papers\n", inserted, len(papers))
		}
	}

	fmt.Println("  Data load complete.")
	return nil
}

// SetupVectorizer enables the vectorize feature and creates a vectorizer on the
// papers table to auto-embed title and abstract columns.
func (s *ChatService) SetupVectorizer(ctx context.Context) error {
	fmt.Println("Creating vectorizer...")

	// Enable vectorize feature.
	if _, err := s.db.ExecContext(ctx,
		`SET CLUSTER SETTING sql.vectorize.enabled = true`,
	); err != nil {
		return fmt.Errorf("enabling vectorize: %w", err)
	}

	// Create vectorizer on title + abstract columns.
	if _, err := s.db.ExecContext(ctx, `
		CREATE VECTORIZER ON papers
		USING COLUMN (title, abstract)
		WITH schedule = '@every 30s'
	`); err != nil {
		if !isAlreadyExists(err) {
			return fmt.Errorf("creating vectorizer: %w", err)
		}
		fmt.Println("  Vectorizer already exists, skipping")
	} else {
		fmt.Println("  Created vectorizer on papers(title, abstract)")
	}

	fmt.Println()
	fmt.Println("Vectorizer will embed all rows in the background.")
	fmt.Println("Check progress with:")
	fmt.Println("  SELECT count(*) FROM papers_embeddings;")

	return nil
}
