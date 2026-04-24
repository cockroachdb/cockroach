package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Source represents a retrieved book chunk with its relevance score.
type Source struct {
	Title    string
	Chunk    string
	Price    float64
	Regions  string
	Distance float64
}

// SearchFilters holds the LLM's analysis of a user message: whether a
// database search is needed and, if so, the semantic query and structured
// filters to apply.
type SearchFilters struct {
	NeedsSearch   bool     `json:"needs_search"`
	SemanticQuery string   `json:"semantic_query"`
	MaxPrice      *float64 `json:"max_price"`
	MinPrice      *float64 `json:"min_price"`
	Region        *string  `json:"region"`
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
		fmt.Print("\n  [No search needed]")
		messages = s.buildDirectPrompt(userMsg, history)
	} else {
		s.printFilters(filters)

		// Step 2: Run filtered semantic search.
		sources, err = s.search(ctx, filters)
		if err != nil {
			return "", nil, fmt.Errorf("search: %w", err)
		}

		if len(sources) == 0 {
			msg := "No matching books found."
			if filters.MaxPrice != nil || filters.MinPrice != nil || filters.Region != nil {
				msg += " Try relaxing your price or region filters."
			} else {
				msg += " Make sure the vectorizer has finished embedding all rows" +
					" (check: SELECT count(*) FROM books_embeddings)."
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

const filterExtractionPrompt = `You are a query classifier and parser for a bookstore chat system. Analyze the user's message and return a JSON object.

First, decide if the message requires searching the book database. Set "needs_search" to false for:
- Greetings, small talk, or thank-you messages
- Follow-up questions that can be answered from the conversation history alone
- Questions unrelated to books (weather, sports, etc.)
- Meta-questions about the chat itself

Set "needs_search" to true when the user is asking about books, topics, prices, or availability.

When "needs_search" is true, also extract:
- "semantic_query": the main topic/subject to search for (string)
- "max_price": maximum price in dollars if mentioned (number or null)
- "min_price": minimum price in dollars if mentioned (number or null)
- "region": region filter if mentioned (string or null). Valid regions: us-east, us-west, eu-west, eu-central, ap-south, ap-east

Examples:
User: "Hello!"
{"needs_search": false, "semantic_query": "", "max_price": null, "min_price": null, "region": null}

User: "Thanks, that was helpful"
{"needs_search": false, "semantic_query": "", "max_price": null, "min_price": null, "region": null}

User: "Can you tell me more about that last book?"
{"needs_search": false, "semantic_query": "", "max_price": null, "min_price": null, "region": null}

User: "What is the weather like?"
{"needs_search": false, "semantic_query": "", "max_price": null, "min_price": null, "region": null}

User: "What books about algorithms cost less than $50?"
{"needs_search": true, "semantic_query": "algorithms", "max_price": 50, "min_price": null, "region": null}

User: "Recommend a database book available in eu-west"
{"needs_search": true, "semantic_query": "database", "max_price": null, "min_price": null, "region": "eu-west"}

User: "What do you have on machine learning?"
{"needs_search": true, "semantic_query": "machine learning", "max_price": null, "min_price": null, "region": null}

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
	if filters.MinPrice != nil {
		parts = append(parts, fmt.Sprintf("min_price=$%.2f", *filters.MinPrice))
	}
	if filters.MaxPrice != nil {
		parts = append(parts, fmt.Sprintf("max_price=$%.2f", *filters.MaxPrice))
	}
	if filters.Region != nil {
		parts = append(parts, fmt.Sprintf("region=%s", *filters.Region))
	}
	fmt.Printf("\n  [Filters: %s]", strings.Join(parts, ", "))
}

// search finds the top-K most relevant book chunks for the given semantic
// query, filtered by any price or region constraints. Uses CockroachDB's
// embed() builtin and cosine distance operator with SQL WHERE clauses.
func (s *ChatService) search(ctx context.Context, filters SearchFilters) ([]Source, error) {
	// Build the WHERE clause dynamically with parameterized placeholders.
	// $1 = semantic query, $2 = limit; additional params start at $3.
	var whereClauses []string
	args := []interface{}{filters.SemanticQuery, s.topK}
	paramIdx := 3

	if filters.MaxPrice != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("b.price <= $%d", paramIdx))
		args = append(args, *filters.MaxPrice)
		paramIdx++
	}
	if filters.MinPrice != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("b.price >= $%d", paramIdx))
		args = append(args, *filters.MinPrice)
		paramIdx++
	}
	if filters.Region != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("$%d::STRING = ANY(b.region)", paramIdx))
		args = append(args, *filters.Region)
		paramIdx++
	}

	whereStr := ""
	if len(whereClauses) > 0 {
		whereStr = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT b.title, e.chunk, b.price,
		       array_to_string(b.region, ', ') AS regions,
		       e.embedding <=> embed($1) AS distance
		FROM books b
		JOIN books_embeddings e ON b.id = e.source_id
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
		if err := rows.Scan(&src.Title, &src.Chunk, &src.Price, &src.Regions, &src.Distance); err != nil {
			return nil, err
		}
		sources = append(sources, src)
	}
	return sources, rows.Err()
}

// buildDirectPrompt constructs a prompt for messages that don't require a
// database search — the LLM responds using only the conversation history.
func (s *ChatService) buildDirectPrompt(userMsg string, history []Message) []Message {
	systemPrompt := `You are a helpful bookstore assistant. The user's message does not require looking up books. Respond naturally and conversationally. If the user asks a follow-up about a book previously discussed, use the conversation history to answer. If you don't have enough information, suggest they ask a book-related question.`

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
			fmt.Sprintf("[%d] %s (Price: $%.2f, Available in: %s):\n%s",
				i+1, src.Title, src.Price, src.Regions, src.Chunk))
	}
	contextStr := strings.Join(contextParts, "\n\n")

	systemPrompt := fmt.Sprintf(`You are a helpful bookstore assistant that answers questions about books using the provided context. Follow these rules:
1. Use ONLY the information from the context below to answer.
2. If the context doesn't contain enough information, say so clearly.
3. Be concise. Cite which book(s) you used by number (e.g., [1], [2]).
4. Include price and regional availability when relevant to the question.
5. Do not make up information that is not in the context.

Context:
%s`, contextStr)

	messages := []Message{{Role: "system", Content: systemPrompt}}
	messages = append(messages, history...)
	messages = append(messages, Message{Role: "user", Content: userMsg})
	return messages
}
