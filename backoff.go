package websocket

import (
	"math"
	"math/rand"
	"time"
)

type backoffFunc func(retry int) time.Duration

func newBackoff(strategy string) backoffFunc {
	switch strategy {
	case "linear":
		return func(retry int) time.Duration {
			if retry < 1 {
				retry = 1
			}
			return time.Duration(retry) * time.Second
		}
	case "linear-jitter":
		return func(retry int) time.Duration {
			if retry < 1 {
				retry = 1
			}
			base := float64(retry)
			jitter := (rand.Float64()*2 - 1) * base * 0.33
			return time.Duration(base+jitter) * time.Second
		}
	case "exponential":
		return func(retry int) time.Duration {
			if retry < 1 {
				retry = 1
			}
			return time.Duration(math.Pow(2, float64(retry))) * time.Second
		}
	case "exponential-jitter":
		return func(retry int) time.Duration {
			if retry < 1 {
				retry = 1
			}
			base := math.Pow(2, float64(retry))
			jitter := (rand.Float64()*2 - 1) * base * 0.33
			return time.Duration(base+jitter) * time.Second
		}
	default:
		return func(_ int) time.Duration {
			return time.Second
		}
	}
}
