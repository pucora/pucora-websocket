package websocket

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// AcceptOptions builds client accept options from config.
func AcceptOptions(cfg Config) *websocket.AcceptOptions {
	return acceptOptions(cfg)
}

func acceptOptions(cfg Config) *websocket.AcceptOptions {
	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}
	if len(cfg.Subprotocols) > 0 {
		opts.Subprotocols = cfg.Subprotocols
	}
	// ReadBufferSize and WriteBufferSize are parsed from config but not applied:
	// github.com/coder/websocket does not expose buffer size knobs on Accept/Dial.
	return opts
}

func dialOptions(cfg Config, headers http.Header) *websocket.DialOptions {
	opts := copyHeadersToDialOpts(headers)
	if len(cfg.Subprotocols) > 0 {
		opts.Subprotocols = cfg.Subprotocols
	}
	if cfg.Timeout > 0 {
		opts.HTTPClient = &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
		}
	}
	return opts
}

func readContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}
