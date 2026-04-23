package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Message represents a chat message with a role and content.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// OllamaClient is a client for the Ollama chat API with streaming support.
type OllamaClient struct {
	baseURL string
	model   string
	client  *http.Client
}

// NewOllamaClient creates a new Ollama client.
func NewOllamaClient(baseURL, model string) *OllamaClient {
	return &OllamaClient{
		baseURL: baseURL,
		model:   model,
		client:  &http.Client{},
	}
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
	Format   string    `json:"format,omitempty"`
}

type streamChunk struct {
	Message Message `json:"message"`
	Done    bool    `json:"done"`
}

// Chat sends a non-streaming chat request and returns the complete response.
// If jsonMode is true, Ollama is instructed to return valid JSON.
func (c *OllamaClient) Chat(
	ctx context.Context, messages []Message, jsonMode bool,
) (string, error) {
	reqBody := chatRequest{
		Model:    c.model,
		Messages: messages,
		Stream:   false,
	}
	if jsonMode {
		reqBody.Format = "json"
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(
		ctx, "POST", c.baseURL+"/api/chat", bytes.NewReader(body),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling Ollama: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("Ollama returned %d: %s", resp.StatusCode, respBody)
	}

	var chatResp struct {
		Message Message `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", fmt.Errorf("decoding response: %w", err)
	}
	return chatResp.Message.Content, nil
}

// ChatStream sends a chat request to Ollama and streams tokens to w as they
// arrive. Returns the full accumulated response.
func (c *OllamaClient) ChatStream(
	ctx context.Context, messages []Message, w io.Writer,
) (string, error) {
	reqBody := chatRequest{
		Model:    c.model,
		Messages: messages,
		Stream:   true,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(
		ctx, "POST", c.baseURL+"/api/chat", bytes.NewReader(body),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling Ollama: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("Ollama returned %d: %s", resp.StatusCode, respBody)
	}

	var full strings.Builder
	decoder := json.NewDecoder(resp.Body)
	for {
		var chunk streamChunk
		if err := decoder.Decode(&chunk); err != nil {
			if err == io.EOF {
				break
			}
			return full.String(), fmt.Errorf("decoding stream: %w", err)
		}
		token := chunk.Message.Content
		full.WriteString(token)
		fmt.Fprint(w, token)
		if chunk.Done {
			break
		}
	}

	return full.String(), nil
}
