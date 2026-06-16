package websocket

import (
	"context"
	"time"

	"github.com/coder/websocket"
)

func PingLoop(ctx context.Context, conn *websocket.Conn, period, pongWait time.Duration) {
	if period <= 0 {
		period = defaultPingPeriod
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, pongWait)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				_ = conn.Close(websocket.StatusNormalClosure, "ping failed")
				return
			}
		}
	}
}
