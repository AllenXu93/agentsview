package sync_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

func TestPiDialogueSearch(t *testing.T) {
	env := setupTestEnv(t)
	content := strings.Join([]string{
		`{"type":"session","version":3,"id":"dialogue","timestamp":"2026-01-01T10:00:00Z","cwd":"/tmp/project"}`,
		`{"type":"message","id":"u","timestamp":"2026-01-01T10:00:01Z","message":{"role":"user","content":"Please inspect this."}}`,
		fmt.Sprintf(`{"type":"message","id":"a","timestamp":"2026-01-01T10:00:02Z","message":{"role":"assistant","model":"claude-opus-4-5","content":[{"type":"thinking","thinking":%q},{"type":"text","text":"answerneedle literal [Thinking] quotation"},{"type":"toolCall","id":"call","name":"bash","arguments":{"command":"commandneedle"}}]}}`, strings.Repeat("reasoning ", 100)+"reasoningneedle"),
		`{"type":"message","id":"u2","timestamp":"2026-01-01T10:00:03Z","message":{"role":"user","content":"Thank you."}}`,
	}, "\n")
	env.writeSession(t, env.piDir, filepath.Join("encoded-cwd", "dialogue.jsonl"), content)
	stats := env.engine.SyncAll(t.Context(), nil)
	require.Equal(t, 1, stats.Synced)
	for _, mode := range []string{"substring", "regex", "fts", "terms"} {
		for _, term := range []string{"answerneedle", "Thinking", "commandneedle", "reasoningneedle"} {
			page, err := env.db.SearchContent(t.Context(), db.ContentSearchFilter{Pattern: term, Mode: mode, Sources: []string{"messages"}})
			require.NoError(t, err)
			if term == "commandneedle" || term == "reasoningneedle" {
				assert.Empty(t, page.Matches)
				continue
			}
			require.Len(t, page.Matches, 1)
			assert.NotContains(t, page.Matches[0].Snippet, "reasoning")
			assert.NotContains(t, page.Matches[0].Snippet, "bash")
		}
	}
	for _, tc := range []struct{ term, source string }{{"commandneedle", "tool_input"}, {"reasoningneedle", "thinking"}} {
		page, err := env.db.SearchContent(t.Context(), db.ContentSearchFilter{Pattern: tc.term, Sources: []string{tc.source}})
		require.NoError(t, err)
		require.Len(t, page.Matches, 1)
	}
}
