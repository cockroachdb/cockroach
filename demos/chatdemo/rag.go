package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Source represents a retrieved paper chunk with its relevance score.
type Source struct {
	Title    string
	Chunk    string
	Authors  string
	Category string
	PDFLink  string
	Distance float64
}

// SearchFilters holds the LLM's analysis of a user message: whether a
// database search is needed and, if so, the semantic query and structured
// filters to apply.
type SearchFilters struct {
	NeedsSearch   bool    `json:"needs_search"`
	SemanticQuery string  `json:"semantic_query"`
	Category      *string `json:"category"`
	MinYear       *int    `json:"min_year"`
	MaxYear       *int    `json:"max_year"`
}

// ChatService ties together CockroachDB semantic search and an Ollama LLM.
type ChatService struct {
	db   *sql.DB
	llm  *OllamaClient
	topK int
}

// NewChatService creates a new chat service connected to CockroachDB and Ollama.
func NewChatService(
	ctx context.Context, dbURL, ollamaURL, model string, topK int,
) (*ChatService, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to CockroachDB: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging CockroachDB: %w", err)
	}

	return &ChatService{
		db:   db,
		llm:  NewOllamaClient(ollamaURL, model),
		topK: topK,
	}, nil
}

// Close releases database resources.
func (s *ChatService) Close() {
	s.db.Close()
}

// Chat performs a full RAG cycle: extract filters from the user's question,
// run a filtered semantic search, and generate a response via the LLM.
func (s *ChatService) Chat(
	ctx context.Context, userMsg string, history []Message,
) (string, []Source, error) {
	// Step 1: Classify the query and extract filters if needed.
	filters, err := s.extractFilters(ctx, userMsg)
	if err != nil {
		// Fall back to unfiltered search on extraction failure.
		fmt.Fprintf(os.Stderr, "\n  (filter extraction failed: %v, using unfiltered search)\n", err)
		filters = SearchFilters{NeedsSearch: true, SemanticQuery: userMsg}
	}

	var sources []Source
	var messages []Message

	if !filters.NeedsSearch {
		// No search needed — respond directly from conversation history.
		messages = s.buildDirectPrompt(userMsg, history)
	} else {
		s.printFilters(filters)

		// Step 2: Run filtered semantic search.
		sources, err = s.search(ctx, filters)
		if err != nil {
			return "", nil, fmt.Errorf("search: %w", err)
		}

		if len(sources) == 0 {
			msg := "No matching papers found."
			if filters.Category != nil || filters.MinYear != nil || filters.MaxYear != nil {
				msg += " Try relaxing your category or year filters."
			} else {
				msg += " Make sure the vectorizer has finished embedding all rows" +
					" (check: SELECT count(*) FROM papers_embeddings)."
			}
			return msg, nil, nil
		}

		// Step 3: Build prompt with retrieved context.
		messages = s.buildPrompt(userMsg, sources, history)
	}

	// Step 4: Stream the LLM response to stdout.
	fmt.Print("\nAssistant: ")
	response, err := s.llm.ChatStream(ctx, messages, os.Stdout)
	if err != nil {
		return "", nil, fmt.Errorf("llm: %w", err)
	}

	return response, sources, nil
}

const filterExtractionPrompt = `You are a query classifier and parser for a CS research paper archive chat system. The archive contains papers from arXiv across categories like cs.AI, cs.CL, cs.CR, cs.CV, cs.DB, cs.DC, cs.DS, cs.LG, cs.SE, and cs.PL. Analyze the user's message and return a JSON object.

First, decide if the message requires searching the paper database. Set "needs_search" to false for:
- Greetings, small talk, or thank-you messages
- Follow-up questions that can be answered from the conversation history alone
- Questions unrelated to CS research (weather, sports, etc.)
- Meta-questions about the chat itself

Set "needs_search" to true when the user is asking about research topics, papers, authors, or CS concepts.

When "needs_search" is true, also extract:
- "semantic_query": the main topic/subject to search for (string)
- "category": arXiv CS category if mentioned (string or null). Valid categories: cs.AI, cs.CL, cs.CR, cs.CV, cs.DB, cs.DC, cs.DS, cs.LG, cs.SE, cs.PL
- "min_year": earliest publication year if mentioned (number or null)
- "max_year": latest publication year if mentioned (number or null)

For relative time expressions like "last year", "recent", "past 2 years", etc., translate them into min_year/max_year based on today's date. For example, if today is 2026, "last year" means min_year=2025 (do NOT set max_year, so current-year papers are included too). "Recent" or "latest" should generally not set any year filter — just let the semantic search find the best matches.

Examples:
User: "Hello!"
{"needs_search": false, "semantic_query": "", "category": null, "min_year": null, "max_year": null}

User: "Thanks, that was helpful"
{"needs_search": false, "semantic_query": "", "category": null, "min_year": null, "max_year": null}

User: "Can you tell me more about that last paper?"
{"needs_search": false, "semantic_query": "", "category": null, "min_year": null, "max_year": null}

User: "What papers are there on transformer architectures?"
{"needs_search": true, "semantic_query": "transformer architectures", "category": null, "min_year": null, "max_year": null}

User: "Find me database papers on query optimization"
{"needs_search": true, "semantic_query": "query optimization", "category": "cs.DB", "min_year": null, "max_year": null}

User: "Recent machine learning papers on reinforcement learning from 2024"
{"needs_search": true, "semantic_query": "reinforcement learning", "category": "cs.LG", "min_year": 2024, "max_year": null}

User: "What research was done on cryptographic protocols before 2020?"
{"needs_search": true, "semantic_query": "cryptographic protocols", "category": "cs.CR", "min_year": null, "max_year": 2020}

User: "Papers published in the last year about AI agents"
{"needs_search": true, "semantic_query": "AI agents", "category": "cs.AI", "min_year": 2025, "max_year": null}

Return ONLY the JSON object, no other text.`

// extractFilters calls the LLM to decompose a natural language query into
// a semantic search query and optional structured filters.
func (s *ChatService) extractFilters(ctx context.Context, userMsg string) (SearchFilters, error) {
	messages := []Message{
		{Role: "system", Content: filterExtractionPrompt},
		{Role: "user", Content: userMsg},
	}

	resp, err := s.llm.Chat(ctx, messages, true /* jsonMode */)
	if err != nil {
		return SearchFilters{}, fmt.Errorf("calling LLM: %w", err)
	}

	var filters SearchFilters
	if err := json.Unmarshal([]byte(resp), &filters); err != nil {
		return SearchFilters{}, fmt.Errorf("parsing filters JSON: %w", err)
	}

	// Ensure we always have a semantic query to search with.
	if filters.SemanticQuery == "" {
		filters.SemanticQuery = userMsg
	}

	return filters, nil
}

// printFilters displays the detected filters to the user.
func (s *ChatService) printFilters(filters SearchFilters) {
	var parts []string
	parts = append(parts, fmt.Sprintf("query=%q", filters.SemanticQuery))
	if filters.Category != nil {
		parts = append(parts, fmt.Sprintf("category=%s", *filters.Category))
	}
	if filters.MinYear != nil {
		parts = append(parts, fmt.Sprintf("min_year=%d", *filters.MinYear))
	}
	if filters.MaxYear != nil {
		parts = append(parts, fmt.Sprintf("max_year=%d", *filters.MaxYear))
	}
	fmt.Printf("\n  [Filters: %s]", strings.Join(parts, ", "))
}

// search finds the top-K most relevant paper chunks for the given semantic
// query, filtered by any category or year constraints. Uses CockroachDB's
// embed() builtin and cosine distance operator with SQL WHERE clauses.
func (s *ChatService) search(ctx context.Context, filters SearchFilters) ([]Source, error) {
	// Build the WHERE clause dynamically with parameterized placeholders.
	// $1 = semantic query, $2 = limit; additional params start at $3.
	var whereClauses []string
	args := []interface{}{filters.SemanticQuery, s.topK}
	paramIdx := 3

	if filters.Category != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("p.category = $%d", paramIdx))
		args = append(args, *filters.Category)
		paramIdx++
	}
	if filters.MinYear != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("EXTRACT(YEAR FROM p.published) >= $%d", paramIdx))
		args = append(args, *filters.MinYear)
		paramIdx++
	}
	if filters.MaxYear != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("EXTRACT(YEAR FROM p.published) <= $%d", paramIdx))
		args = append(args, *filters.MaxYear)
		paramIdx++
	}

	whereStr := ""
	if len(whereClauses) > 0 {
		whereStr = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT p.title, e.chunk, p.authors, p.category, p.pdf_link,
		       e.embedding <=> embed($1) AS distance
		FROM papers p
		JOIN papers_embeddings e ON p.id = e.source_id
		%s
		ORDER BY distance ASC
		LIMIT $2
	`, whereStr)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []Source
	for rows.Next() {
		var src Source
		if err := rows.Scan(
			&src.Title, &src.Chunk, &src.Authors,
			&src.Category, &src.PDFLink, &src.Distance,
		); err != nil {
			return nil, err
		}
		sources = append(sources, src)
	}
	return sources, rows.Err()
}

// buildDirectPrompt constructs a prompt for messages that don't require a
// database search — the LLM responds using only the conversation history.
func (s *ChatService) buildDirectPrompt(userMsg string, history []Message) []Message {
	systemPrompt := `You are a helpful CS research paper archive assistant. The user's message does not require looking up papers. Respond naturally and conversationally. If the user asks a follow-up about a paper previously discussed, use the conversation history to answer. If you don't have enough information, suggest they ask a research-related question.`

	messages := []Message{{Role: "system", Content: systemPrompt}}
	messages = append(messages, history...)
	messages = append(messages, Message{Role: "user", Content: userMsg})
	return messages
}

// buildPrompt constructs the LLM messages array with a system prompt containing
// retrieved context, the conversation history, and the current user question.
func (s *ChatService) buildPrompt(userMsg string, sources []Source, history []Message) []Message {
	var contextParts []string
	for i, src := range sources {
		contextParts = append(contextParts,
			fmt.Sprintf("[%d] %s\n    Authors: %s\n    Category: %s\n    PDF: %s\n    %s",
				i+1, src.Title, src.Authors, src.Category, src.PDFLink, src.Chunk))
	}
	contextStr := strings.Join(contextParts, "\n\n")

	systemPrompt := fmt.Sprintf(`You are a helpful CS research paper archive assistant that answers questions about computer science research using the provided context. Follow these rules:
1. Use ONLY the information from the context below to answer.
2. If the context doesn't contain enough information, say so clearly.
3. Be concise. Cite which paper(s) you used by number (e.g., [1], [2]).
4. Include the paper's authors and PDF link when relevant to the question.
5. Do not make up information that is not in the context.

Context:
%s`, contextStr)

	messages := []Message{{Role: "system", Content: systemPrompt}}
	messages = append(messages, history...)
	messages = append(messages, Message{Role: "user", Content: userMsg})
	return messages
}
