package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianbuquoi/overlord/internal/auth"
	"github.com/brianbuquoi/overlord/internal/broker"
	"github.com/brianbuquoi/overlord/internal/contract"
	"github.com/brianbuquoi/overlord/internal/store/memory"
)

// The auth middleware must only admit the first caller that races to
// consume a ws-token. Subsequent callers using the same token must be
// rejected.
func TestWSToken_ConcurrentUpgrade(t *testing.T) {
	s := newWSTokenStore()
	tok, _, err := s.issue()
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	const N = 20
	var admitted, rejected atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.consume(tok) {
				admitted.Add(1)
			} else {
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()

	if admitted.Load() != 1 {
		t.Fatalf("admitted: got %d, want 1", admitted.Load())
	}
	if rejected.Load() != N-1 {
		t.Fatalf("rejected: got %d, want %d", rejected.Load(), N-1)
	}
	// Token should now be fully consumed.
	if s.consume(tok) {
		t.Fatal("token should be fully consumed after the winner")
	}
}

func TestWSTokenStore_IssueAndConsumeOnce(t *testing.T) {
	s := newWSTokenStore()
	tok, ttl, err := s.issue()
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}
	if ttl <= 0 {
		t.Fatalf("ttl: got %d, want > 0", ttl)
	}
	if !s.consume(tok) {
		t.Fatal("first consume should succeed")
	}
	if s.consume(tok) {
		t.Fatal("second consume must fail — token is single-use")
	}
}

func TestWSTokenStore_RejectsUnknownToken(t *testing.T) {
	s := newWSTokenStore()
	if s.consume("not-a-real-token") {
		t.Fatal("unknown token must not be accepted")
	}
	if s.consume("") {
		t.Fatal("empty token must not be accepted")
	}
}

func TestWSTokenStore_ExpiredTokenRejected(t *testing.T) {
	s := newWSTokenStore()
	now := time.Now()
	s.now = func() time.Time { return now }
	tok, _, err := s.issue()
	if err != nil {
		t.Fatal(err)
	}
	// Jump well past the TTL.
	now = now.Add(s.ttl + time.Second)
	if s.consume(tok) {
		t.Fatal("expired token must be rejected")
	}
}

func TestWSTokenStore_PrunesExpiredOnIssue(t *testing.T) {
	s := newWSTokenStore()
	now := time.Now()
	s.now = func() time.Time { return now }
	for i := 0; i < 10; i++ {
		if _, _, err := s.issue(); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(s.ttl + time.Second)
	// Next issue should prune the 10 expired tokens.
	if _, _, err := s.issue(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	remaining := len(s.tokens)
	s.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("expected 1 live token after prune, got %d", remaining)
	}
}

// newIssueServer returns a minimal Server with a text-handler slog logger
// whose output is captured in buf, plus the wsTokens store pre-initialized.
func newIssueServer(t *testing.T, buf *bytes.Buffer) *Server {
	t.Helper()
	cfg := testConfig()
	st := memory.New()
	reg, _ := contract.NewRegistry(nil, "")
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := broker.New(cfg, st, nil, reg, logger, nil, nil)
	srv := NewServer(b, logger, nil, "")
	srv.wsTokens = newWSTokenStore()
	return srv
}

// TestWSToken_IssuanceLogIncludesClientIP asserts that the issuance log
// contains the client_ip field and not the token value.
func TestWSToken_IssuanceLogIncludesClientIP(t *testing.T) {
	var buf bytes.Buffer
	srv := newIssueServer(t, &buf)

	req := httptest.NewRequest(http.MethodPost, "/v1/ws-token", nil)
	req.RemoteAddr = "192.0.2.45:54321"
	rr := httptest.NewRecorder()
	srv.handleIssueWSToken(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	logs := buf.String()
	if !strings.Contains(logs, "ws-token issued") {
		t.Fatalf("expected issuance log, got:\n%s", logs)
	}
	if !strings.Contains(logs, "client_ip=192.0.2.45") {
		t.Errorf("issuance log missing client_ip=192.0.2.45:\n%s", logs)
	}
	// Ensure the token value itself is not in the log. Extract it from the
	// response body and confirm it isn't echoed in the captured logs.
	respBody := rr.Body.String()
	const tokKey = `"token":"`
	i := strings.Index(respBody, tokKey)
	if i < 0 {
		t.Fatal("response missing token field")
	}
	rest := respBody[i+len(tokKey):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		t.Fatal("malformed token field in response")
	}
	tokenValue := rest[:j]
	if tokenValue == "" {
		t.Fatal("empty token in response")
	}
	if strings.Contains(logs, tokenValue) {
		t.Error("issuance log must not contain the token value")
	}
}

// TestWSToken_RejectionLogIncludesClientIP asserts that a failed WebSocket
// upgrade with an invalid ws-token produces a WARN log with client_ip and
// never echoes the token value.
func TestWSToken_RejectionLogIncludesClientIP(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	wsTokens := newWSTokenStore()
	// Build middleware with an empty key set; WS upgrade + ws-token path is
	// what we are exercising.
	tracker := auth.NewBruteForceTracker(100, time.Minute)
	mw := authMiddleware(nil, tracker, logger, func(r *http.Request) auth.Scope { return auth.ScopeRead }, wsTokens)

	// Stub next handler: should not be reached for rejected tokens.
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})

	const bogusToken = "not-a-real-token-abcdef0123456789"
	target := "/v1/stream?" + url.Values{"token": []string{bogusToken}}.Encode()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.RemoteAddr = "198.51.100.7:12000"
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")

	rr := httptest.NewRecorder()
	mw(next).ServeHTTP(rr, req)

	if reached {
		t.Fatal("rejected upgrade should not reach next handler")
	}
	logs := buf.String()
	if !strings.Contains(logs, "ws-token rejected") {
		t.Fatalf("expected rejection log, got:\n%s", logs)
	}
	if !strings.Contains(logs, "client_ip=198.51.100.7") {
		t.Errorf("rejection log missing client_ip=198.51.100.7:\n%s", logs)
	}
	if strings.Contains(logs, bogusToken) {
		t.Error("rejection log must not contain the token value")
	}
}
