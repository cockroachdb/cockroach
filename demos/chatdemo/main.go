// chatdemo is a terminal-based RAG chat application that performs semantic
// search over CS research papers stored in CockroachDB using the vectorizer
// feature, and generates responses using a local Ollama LLM.
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
	topK := flag.Int("top-k", 5, "Number of paper chunks to retrieve")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Handle "setup" subcommand: --setup, --setup load, --setup vectorizer.
	if args := flag.Args(); len(args) > 0 && args[0] == "setup" {
		chat, err := NewChatService(ctx, *dbURL, "", "", 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer chat.Close()

		subCmd := "all"
		if len(args) > 1 {
			subCmd = args[1]
		}
		var setupErr error
		switch subCmd {
		case "load":
			setupErr = chat.SetupLoad(ctx)
		case "vectorizer":
			setupErr = chat.SetupVectorizer(ctx)
		case "all":
			setupErr = chat.SetupAll(ctx)
		default:
			fmt.Fprintf(os.Stderr,
				"Unknown setup subcommand %q. Usage: setup [load|vectorizer]\n", subCmd)
			os.Exit(1)
		}
		if setupErr != nil {
			fmt.Fprintf(os.Stderr, "Setup error: %v\n", setupErr)
			os.Exit(1)
		}
		return
	}

	chat, err := NewChatService(ctx, *dbURL, *ollamaURL, *model, *topK)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer chat.Close()

	fmt.Println("Chat with the CS research paper archive (type 'quit' to exit)")
	fmt.Println("Semantic search powered by CockroachDB vectorizer")
	fmt.Println("LLM: " + *model + " via Ollama")
	fmt.Println(strings.Repeat("-", 55))

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
				fmt.Printf("  - %s [%s] (distance: %.3f)\n    PDF: %s\n",
					src.Title, src.Category, src.Distance, src.PDFLink)
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
