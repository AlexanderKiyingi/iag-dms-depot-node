package config

import (
	"os"
	"strings"
)

// ListenAddr resolves the bind address from PORT (Railway) or ADDR, defaulting
// to :4020 — the reserved edge-node port range.
func ListenAddr() string {
	if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
		return normalizeListenAddr(p)
	}
	if v := strings.TrimSpace(os.Getenv("ADDR")); v != "" {
		return normalizeListenAddr(v)
	}
	return ":4020"
}

func normalizeListenAddr(addr string) string {
	if !strings.HasPrefix(addr, ":") && !strings.Contains(addr, ":") {
		return ":" + addr
	}
	return addr
}
