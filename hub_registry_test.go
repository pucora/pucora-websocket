package websocket

import (
	"testing"
)

func TestResetHubRegistry(t *testing.T) {
	hub := &Hub{
		endpoint: "/test-reset",
		cfg:      Config{MessageBufferSize: 2},
		clients:  make(map[string]*ClientSession),
		backoff:  newBackoff("fallback"),
	}
	hubRegistry.Store("/test-reset", hub)
	hub.clients["c1"] = newClientSession("c1", "/test-reset", nil, nil, 2)

	ResetHubRegistry()

	if _, ok := hubRegistry.Load("/test-reset"); ok {
		t.Fatal("expected hub to be removed from registry")
	}
}

func TestHubRegistryReuseAfterReset(t *testing.T) {
	t.Cleanup(ResetHubRegistry)

	cfg := Config{MessageBufferSize: 2, BackoffStrategy: "fallback"}
	h1 := GetHub("/reuse", cfg, nil)
	h2 := GetHub("/reuse", cfg, nil)
	if h1 != h2 {
		t.Fatal("expected same hub instance before reset")
	}

	ResetHubRegistry()

	h3 := GetHub("/reuse", cfg, nil)
	if h3 == h1 {
		t.Fatal("expected new hub instance after reset")
	}
}
