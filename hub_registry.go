package websocket

// ResetHubRegistry clears multiplex hubs between tests.
// It is intended for test isolation only.
func ResetHubRegistry() {
	hubRegistry.Range(func(key, value interface{}) bool {
		h := value.(*Hub)
		h.markBackendDown()
		h.mu.Lock()
		for id, client := range h.clients {
			delete(h.clients, id)
			client.close()
		}
		h.mu.Unlock()
		hubRegistry.Delete(key)
		return true
	})
}
