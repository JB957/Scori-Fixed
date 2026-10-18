package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func checkConfig(url, matchType, expected string) string {
	data, _ := json.Marshal(map[string]any{
		"url": url, "verb": "GET", "match_type": matchType,
		"expected_output": expected, "content_type": "empty",
		"insecure": false, "headers": "", "body": "",
	})
	return string(data)
}

func TestResponseLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := Run(ctx, checkConfig(server.URL, "substringMatch", "x")); err == nil || !strings.Contains(err.Error(), "exceeds 1 MiB") {
		t.Fatalf("expected a bounded response error, got %v", err)
	}
}

func TestCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := Run(ctx, checkConfig(server.URL, "statusCode", "200")); err == nil {
		t.Fatal("expected cancellation error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("HTTP check did not stop promptly after cancellation")
	}
}
