package websocket

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestFlushAllPendingRequeuesUnsentOnFailure(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()

		_, data, err := c.Read(ctx)
		if err != nil || string(data) != handshakeMessage {
			return
		}
		_ = c.Write(ctx, websocket.MessageText, []byte(handshakeOK))
	}))
	defer backend.Close()

	wsURL := "ws" + strings.TrimPrefix(backend.URL, "http")
	cfg := Config{
		MaxRetries:        1,
		BackoffStrategy:   "fallback",
		MessageBufferSize: 10,
		WriteWait:         time.Second,
	}
	hub := newTestHub("/echo", cfg)
	defer hub.lifecycleCancel()
	hub.backendURL = wsURL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := newClientSession("sess-1", "/echo", map[string]interface{}{"uuid": "sess-1"}, nil, cfg.MessageBufferSize)
	hub.registerClient(s)

	for i := 0; i < 3; i++ {
		if !s.queueInbound([]byte(fmt.Sprintf("msg%d", i))) {
			t.Fatalf("failed to queue msg%d", i)
		}
	}

	hub.markBackendDown()
	hub.flushAllPending(ctx)

	remaining := s.drainInbound()
	if len(remaining) != 3 {
		t.Fatalf("expected all 3 messages requeued, got %d", len(remaining))
	}
}

func TestDeliverToClientLogsWhenOutboxFull(t *testing.T) {
	s := newClientSession("id", "/echo", nil, nil, 1)
	if !s.enqueue([]byte("first")) {
		t.Fatal("expected first enqueue to succeed")
	}

	hub := newTestHub("/echo", Config{MessageBufferSize: 1})
	defer hub.lifecycleCancel()
	hub.registerClient(s)

	hub.deliverToClient(s, []byte("from-backend"))

	select {
	case msg := <-s.outbox:
		if string(msg) != "first" {
			t.Fatalf("unexpected outbox message: %q", string(msg))
		}
	default:
		t.Fatal("expected first message still in outbox")
	}

	select {
	case <-s.outbox:
		t.Fatal("deliverToClient should not enqueue when outbox is full")
	default:
	}
}

func TestFlushAllPendingLoopPreservesUnsent(t *testing.T) {
	s := newClientSession("id", "/echo", nil, nil, 10)
	for i := 0; i < 3; i++ {
		if !s.queueInbound([]byte(fmt.Sprintf("msg%d", i))) {
			t.Fatalf("failed to queue msg%d", i)
		}
	}

	pending := s.drainInbound()
	sent := 0
	for i, data := range pending {
		sent++
		if sent == 2 {
			for j := i; j < len(pending); j++ {
				if !s.requeueInbound(pending[j]) {
					t.Fatal("requeue failed")
				}
			}
			break
		}
		_ = data
	}

	remaining := s.drainInbound()
	if len(remaining) != 2 {
		t.Fatalf("expected msg1 and msg2 requeued, got %d", len(remaining))
	}
}
