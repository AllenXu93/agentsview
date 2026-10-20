package parser

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGptmeProviderParsesFixture(t *testing.T) {
	logsDir := filepath.Join("testdata", "gptme")

	provider, ok := NewProvider(AgentGptme, ProviderConfig{
		Roots:   []string{logsDir},
		Machine: "testmachine",
	})
	require.True(t, ok)
	source, found, err := provider.FindSource(t.Context(), FindSourceRequest{
		RawSessionID: "2026-06-13-write-hello-world",
	})
	require.NoError(t, err)
	require.True(t, found)
	outcome, err := provider.Parse(t.Context(), ParseRequest{
		Source:  source,
		Machine: "testmachine",
	})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)

	sess := outcome.Results[0].Result.Session
	msgs := outcome.Results[0].Result.Messages
	assert.Equal(t, "gptme:2026-06-13-write-hello-world", sess.ID)
	assert.Equal(t, "write-hello-world", sess.Project)
	assert.Equal(t, "testmachine", sess.Machine)
	assert.Equal(t, AgentGptme, sess.Agent)
	assert.Contains(t, sess.FirstMessage, "hello world")

	// Expect: user, assistant, visible tool output, user, assistant, visible tool output
	// System message is skipped.
	require.Len(t, msgs, 6)

	user0 := msgs[0]
	assert.Equal(t, RoleUser, user0.Role)
	assert.False(t, user0.IsSystem)
	assert.Contains(t, user0.Content, "hello world")

	asst0 := msgs[1]
	assert.Equal(t, RoleAssistant, asst0.Role)
	assert.Equal(t, "openrouter/anthropic/claude-sonnet-4-6", asst0.Model)
	assert.Equal(t, 42, asst0.OutputTokens)
	assert.True(t, asst0.HasOutputTokens)
	assert.Equal(t, 120+80, asst0.ContextTokens) // input + cache_read
	assert.True(t, asst0.HasContextTokens)

	tool0 := msgs[2]
	assert.Equal(t, RoleAssistant, tool0.Role)
	assert.False(t, tool0.IsSystem)
	assert.Contains(t, tool0.Content, "Saved file")
	assert.Equal(t, SourceSubtypeToolResult, tool0.SourceSubtype,
		"tool output kept as assistant text is still tool output")

	// Timestamps must parse from the fixture's microsecond format ("2006-01-02T15:04:05.000000").
	// sess.StartedAt comes from the system message (processed before role-skip).
	assert.Equal(t, time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC), sess.StartedAt)
	assert.Equal(t, time.Date(2026, 6, 13, 10, 0, 13, 0, time.UTC), sess.EndedAt)
	assert.Equal(t, time.Date(2026, 6, 13, 10, 0, 1, 0, time.UTC), msgs[0].Timestamp)

	// Accumulated session totals.
	assert.Equal(t, 42+15, sess.TotalOutputTokens)
	assert.Equal(t, 2, sess.UserMessageCount)
}

func TestGptmeProviderDiscoversFixture(t *testing.T) {
	logsDir := filepath.Join("testdata", "gptme")
	provider, ok := NewProvider(AgentGptme, ProviderConfig{Roots: []string{logsDir}})
	require.True(t, ok)

	sources, err := provider.Discover(t.Context())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, AgentGptme, sources[0].Provider)
	assert.Contains(t, sources[0].DisplayPath, "conversation.jsonl")
}

func TestGptmeProviderFindsFixtureSource(t *testing.T) {
	logsDir := filepath.Join("testdata", "gptme")
	provider, ok := NewProvider(AgentGptme, ProviderConfig{Roots: []string{logsDir}})
	require.True(t, ok)

	found, ok, err := provider.FindSource(t.Context(), FindSourceRequest{
		RawSessionID: "2026-06-13-write-hello-world",
	})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Contains(t, found.DisplayPath, "conversation.jsonl")

	_, ok, err = provider.FindSource(t.Context(), FindSourceRequest{
		RawSessionID: "nonexistent-session",
	})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestGptmeProjectFromSessionName(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"2026-06-13-write-hello-world", "write-hello-world"},
		{"2026-06-13-162241-feat-tts-fix", "feat-tts-fix"},
		{"2026-06-13-my-project-longer", "my-project-longer"},
		{"no-date-here", "no-date-here"},
		{"2026-06-13", "2026-06-13"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := gptmeProjectFromSessionName(c.name)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestGptmeModelID(t *testing.T) {
	cases := []struct {
		model string
		want  string
	}{
		{"openrouter/anthropic/claude-sonnet-4-6", "openrouter/anthropic/claude-sonnet-4-6"},
		{"openrouter/z-ai/glm-5.3-flash@z-ai", "openrouter/z-ai/glm-5.3-flash"},
		{"openrouter/deepseek/deepseek-v4.1-flash@cloudflare,inference-net", "openrouter/deepseek/deepseek-v4.1-flash"},
		{"anthropic/claude-fable-5", "anthropic/claude-fable-5"},
		{"local/model", "local/model"},
	}
	for _, c := range cases {
		t.Run(c.model, func(t *testing.T) {
			assert.Equal(t, c.want, gptmeModelID(c.model))
		})
	}
}

func TestGptmeProviderStripsProviderRouting(t *testing.T) {
	logsDir := t.TempDir()
	sessionDir := filepath.Join(logsDir, "2026-10-19-routed-model")
	require.NoError(t, os.MkdirAll(sessionDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(sessionDir, "conversation.jsonl"),
		[]byte(`{"role": "user", "content": "hi", "timestamp": "2026-10-19T10:00:00.000000"}
{"role": "assistant", "content": "hello", "timestamp": "2026-10-19T10:00:01.000000", "metadata": {"model": "openrouter/z-ai/glm-5.3-flash@z-ai", "usage": {"input_tokens": 10, "output_tokens": 2}}}
`),
		0o644,
	))

	provider, ok := NewProvider(AgentGptme, ProviderConfig{
		Roots:   []string{logsDir},
		Machine: "testmachine",
	})
	require.True(t, ok)
	source, found, err := provider.FindSource(t.Context(), FindSourceRequest{
		RawSessionID: "2026-10-19-routed-model",
	})
	require.NoError(t, err)
	require.True(t, found)
	outcome, err := provider.Parse(t.Context(), ParseRequest{
		Source:  source,
		Machine: "testmachine",
	})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)

	msgs := outcome.Results[0].Result.Messages
	require.Len(t, msgs, 2)
	assert.Equal(t, "openrouter/z-ai/glm-5.3-flash", msgs[1].Model)
}

func TestGptmeExactCostEvent(t *testing.T) {
	logsDir := t.TempDir()
	sessionDir := filepath.Join(logsDir, "2026-10-20-provider-cost")
	require.NoError(t, os.MkdirAll(sessionDir, 0o755))

	// One message with provider-reported cost, one with estimate only.
	require.NoError(t, os.WriteFile(
		filepath.Join(sessionDir, "conversation.jsonl"),
		[]byte(`{"role":"user","content":"hi","timestamp":"2026-10-20T10:00:00.000000"}
{"role":"assistant","content":"hello","timestamp":"2026-10-20T10:00:01.000000","metadata":{"model":"openrouter/deepseek/deepseek-v4.1-flash@cloudflare","cost":0.000123,"cost_source":"provider","usage":{"input_tokens":100,"output_tokens":20,"cache_read_tokens":50,"cache_creation_tokens":0}}}
{"role":"user","content":"again","timestamp":"2026-10-20T10:00:02.000000"}
{"role":"assistant","content":"sure","timestamp":"2026-10-20T10:00:03.000000","metadata":{"model":"openrouter/deepseek/deepseek-v4.1-flash@cloudflare","usage":{"input_tokens":120,"output_tokens":8}}}
`),
		0o644,
	))

	provider, ok := NewProvider(AgentGptme, ProviderConfig{
		Roots:   []string{logsDir},
		Machine: "testmachine",
	})
	require.True(t, ok)
	source, found, err := provider.FindSource(t.Context(), FindSourceRequest{
		RawSessionID: "2026-10-20-provider-cost",
	})
	require.NoError(t, err)
	require.True(t, found)
	outcome, err := provider.Parse(t.Context(), ParseRequest{
		Source:  source,
		Machine: "testmachine",
	})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)
	result := outcome.Results[0].Result

	// Provider-cost message: no token usage on the message row.
	msgs := result.Messages
	require.Len(t, msgs, 4)
	providerMsg := msgs[1]
	assert.Equal(t, "openrouter/deepseek/deepseek-v4.1-flash", providerMsg.Model)
	assert.Nil(t, providerMsg.TokenUsage, "provider-cost message must carry no TokenUsage to avoid DB double-count")
	assert.True(t, providerMsg.HasOutputTokens)
	assert.Equal(t, 20, providerMsg.OutputTokens)
	assert.True(t, providerMsg.HasContextTokens)
	assert.Equal(t, 150, providerMsg.ContextTokens)

	// Estimate-only message still carries per-message token data.
	estimateMsg := msgs[3]
	assert.True(t, estimateMsg.HasOutputTokens)
	assert.Equal(t, 8, estimateMsg.OutputTokens)

	// Exactly one usage event for the provider-cost message.
	require.Len(t, result.UsageEvents, 1)
	ev := result.UsageEvents[0]
	assert.Equal(t, "exact", ev.CostStatus)
	assert.Equal(t, "gptme-provider-cost", ev.CostSource)
	require.NotNil(t, ev.Cost)
	assert.Equal(t, int64(123), ev.Cost.Microdollars)
	assert.Equal(t, 100, ev.InputTokens)
	assert.Equal(t, 20, ev.OutputTokens)
	assert.Equal(t, 50, ev.CacheReadInputTokens)
	assert.Equal(t, 0, ev.CacheCreationInputTokens)
	assert.Equal(t, "openrouter/deepseek/deepseek-v4.1-flash", ev.Model)

	// Session totals include both the event-derived provider tokens and the
	// per-message estimate tokens.
	sess := result.Session
	assert.Equal(t, 20+8, sess.TotalOutputTokens, "provider event + estimate message")
	assert.True(t, sess.HasTotalOutputTokens)
	assert.Equal(t, 150, sess.PeakContextTokens)
	assert.True(t, sess.HasPeakContextTokens)
}
