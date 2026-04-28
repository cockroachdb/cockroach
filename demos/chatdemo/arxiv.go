package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	arxivAPIBase = "http://export.arxiv.org/api/query"
	fetchBatch   = 100
	requestDelay = 3 * time.Second
)

// csCategories lists the arXiv CS subcategories to fetch papers from.
var csCategories = []string{
	"cs.AI", // Artificial Intelligence
	"cs.CL", // Computation and Language
	"cs.CR", // Cryptography and Security
	"cs.CV", // Computer Vision and Pattern Recognition
	"cs.DB", // Databases
	"cs.DC", // Distributed, Parallel, and Cluster Computing
	"cs.DS", // Data Structures and Algorithms
	"cs.LG", // Machine Learning
	"cs.SE", // Software Engineering
	"cs.PL", // Programming Languages
}

const papersPerCategory = 50

// paper holds metadata for a single arXiv CS paper.
type paper struct {
	ArxivID   string
	Title     string
	Authors   string
	Abstract  string
	Category  string
	PDFLink   string
	Published time.Time
}

// arXiv Atom feed XML structures.

type arxivFeed struct {
	XMLName xml.Name     `xml:"feed"`
	Entries []arxivEntry `xml:"entry"`
}

type arxivEntry struct {
	ID         string          `xml:"id"`
	Title      string          `xml:"title"`
	Summary    string          `xml:"summary"`
	Authors    []arxivAuthor   `xml:"author"`
	Published  string          `xml:"published"`
	Links      []arxivLink     `xml:"link"`
	Categories []arxivCategory `xml:"category"`
}

type arxivAuthor struct {
	Name string `xml:"name"`
}

type arxivLink struct {
	Href  string `xml:"href,attr"`
	Title string `xml:"title,attr"`
	Rel   string `xml:"rel,attr"`
	Type  string `xml:"type,attr"`
}

type arxivCategory struct {
	Term string `xml:"term,attr"`
}

// fetchAllPapers downloads ~papersPerCategory papers from each CS category,
// deduplicates by arXiv ID, and returns the combined set.
func fetchAllPapers(ctx context.Context) ([]paper, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	var allPapers []paper
	seen := make(map[string]bool)

	for _, cat := range csCategories {
		fmt.Printf("  Fetching %s papers...\n", cat)
		catCount := 0

		for start := 0; start < papersPerCategory; start += fetchBatch {
			remaining := papersPerCategory - start
			if remaining > fetchBatch {
				remaining = fetchBatch
			}

			papers, err := fetchPaperBatch(ctx, client, cat, start, remaining)
			if err != nil {
				return nil, fmt.Errorf("fetching %s (offset %d): %w", cat, start, err)
			}

			for _, p := range papers {
				if !seen[p.ArxivID] {
					seen[p.ArxivID] = true
					allPapers = append(allPapers, p)
					catCount++
				}
			}

			if len(papers) < remaining {
				// No more results in this category.
				break
			}

			// Respect arXiv rate limits.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(requestDelay):
			}
		}
		fmt.Printf("    %s: %d papers fetched\n", cat, catCount)
	}

	return allPapers, nil
}

// fetchPaperBatch fetches a single page of results from the arXiv API.
func fetchPaperBatch(
	ctx context.Context, client *http.Client, category string, start, maxResults int,
) ([]paper, error) {
	params := url.Values{
		"search_query": {fmt.Sprintf("cat:%s", category)},
		"start":        {fmt.Sprintf("%d", start)},
		"max_results":  {fmt.Sprintf("%d", maxResults)},
		"sortBy":       {"submittedDate"},
		"sortOrder":    {"descending"},
	}

	reqURL := arxivAPIBase + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("arXiv API returned %d: %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var feed arxivFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("parsing XML: %w", err)
	}

	var papers []paper
	for _, entry := range feed.Entries {
		p := entryToPaper(entry, category)
		if p.ArxivID != "" {
			papers = append(papers, p)
		}
	}

	return papers, nil
}

// entryToPaper converts an arXiv Atom entry into our paper struct.
func entryToPaper(entry arxivEntry, primaryCategory string) paper {
	// Extract arXiv ID from the entry URL.
	// e.g. "http://arxiv.org/abs/2301.12345v1" -> "2301.12345"
	arxivID := entry.ID
	if idx := strings.LastIndex(arxivID, "/abs/"); idx >= 0 {
		arxivID = arxivID[idx+5:]
	}
	// Strip version suffix (e.g. "v1", "v2").
	if idx := strings.LastIndex(arxivID, "v"); idx > 0 {
		arxivID = arxivID[:idx]
	}

	// Skip error entries returned by arXiv for bad queries.
	if strings.Contains(entry.ID, "/api/errors") || arxivID == "" {
		return paper{}
	}

	pdfLink := fmt.Sprintf("https://arxiv.org/pdf/%s", arxivID)

	var authorNames []string
	for _, a := range entry.Authors {
		authorNames = append(authorNames, strings.TrimSpace(a.Name))
	}
	authors := strings.Join(authorNames, ", ")

	published, _ := time.Parse(time.RFC3339, entry.Published)

	// Collapse whitespace/newlines in title and abstract.
	title := strings.Join(strings.Fields(entry.Title), " ")
	abstract := strings.Join(strings.Fields(entry.Summary), " ")

	return paper{
		ArxivID:   arxivID,
		Title:     title,
		Authors:   authors,
		Abstract:  abstract,
		Category:  primaryCategory,
		PDFLink:   pdfLink,
		Published: published,
	}
}
