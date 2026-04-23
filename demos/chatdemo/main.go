// chatdemo is a terminal-based RAG chat application that performs semantic
// search over books stored in CockroachDB using the vectorizer feature,
// and generates responses using a local Ollama LLM.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	_ "github.com/lib/pq"
)

func main() {
	dbURL := flag.String("db",
		"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
		"CockroachDB connection URL")
	ollamaURL := flag.String("ollama",
		"http://localhost:11434",
		"Ollama API URL")
	model := flag.String("model", "llama3.1", "Ollama model name")
	setup := flag.Bool("setup", false,
		"Set up demo dataset and vectorizer, then exit")
	topK := flag.Int("top-k", 5, "Number of book chunks to retrieve")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	chat, err := NewChatService(ctx, *dbURL, *ollamaURL, *model, *topK)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer chat.Close()

	if *setup {
		if err := chat.SetupDemo(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Setup error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Println("Chat with your bookstore (type 'quit' to exit)")
	fmt.Println("Semantic search powered by CockroachDB vectorizer")
	fmt.Println("LLM: " + *model + " via Ollama")
	fmt.Println(strings.Repeat("-", 50))

	scanner := bufio.NewScanner(os.Stdin)
	var history []Message

	for {
		fmt.Print("\nYou: ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "quit" || input == "exit" {
			break
		}

		response, sources, err := chat.Chat(ctx, input, history)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
			continue
		}
		fmt.Println() // newline after streamed response

		if len(sources) > 0 {
			fmt.Println("\nSources:")
			seen := make(map[string]bool)
			for _, src := range sources {
				if seen[src.Title] {
					continue
				}
				seen[src.Title] = true
				fmt.Printf("  - %s ($%.2f, %s) [distance: %.3f]\n",
					src.Title, src.Price, src.Regions, src.Distance)
			}
		}

		history = append(history,
			Message{Role: "user", Content: input},
			Message{Role: "assistant", Content: response},
		)
		// Keep history bounded to avoid exceeding LLM context.
		if len(history) > 10 {
			history = history[len(history)-10:]
		}
	}

	fmt.Println("\nGoodbye!")
}
