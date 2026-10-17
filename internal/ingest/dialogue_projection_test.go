package ingest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/ingest"
	"go.kenn.io/agentsview/internal/parser"
)

func TestPiSourceReparseSeparatesSearchContent(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "dialogue.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(`{"type":"session","version":3,"id":"dialogue","timestamp":"2026-05-01T10:00:00Z"}
{"type":"message","id":"u","message":{"role":"user","content":[{"type":"text","text":"Question"}]}}
{"type":"message","id":"a","message":{"role":"assistant","content":[{"type":"text","text":"answerneedle Literal [Read: quotedneedle.txt]"},{"type":"thinking","thinking":"reasoningneedle"},{"type":"toolCall","id":"call-a","name":"read","arguments":{"file_path":"inputneedle.txt"}}]}}
{"type":"message","id":"r","message":{"role":"toolResult","toolCallId":"call-a","toolName":"read","content":[{"type":"text","text":"resultneedle"}]}}
`), 0o600))
	provider, ok := parser.NewProvider(parser.AgentPi, parser.ProviderConfig{Roots: []string{root}, Machine: "local"})
	require.True(t, ok)
	destination := dbtest.OpenTestDB(t)
	var messageID int64
	for _, reparse := range []bool{false, true} {
		outcome, err := provider.Parse(t.Context(), parser.ParseRequest{Source: parser.SourceRef{
			Provider: parser.AgentPi, DisplayPath: path, FingerprintKey: path,
			Opaque: parser.JSONLSource{Root: root, Path: path},
		}})
		require.NoError(t, err)
		require.Len(t, outcome.Results, 1)
		candidate, err := ingest.PrepareCandidate(t.Context(), outcome.Results[0].Result, ingest.ContentOptions{})
		require.NoError(t, err)
		prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
		require.NoError(t, err)
		if !reparse {
			// Model the flattened rows written before dialogue classification.
			for i := range prepared.Messages {
				prepared.Messages[i].DialogueText = nil
			}
		}
		_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: prepared.Messages, ReplaceMessages: true}})
		require.NoError(t, err)
		stored, err := destination.GetAllMessages(t.Context(), prepared.Session.ID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(stored), 2)
		if !reparse {
			messageID = stored[1].ID
		} else {
			assert.Equal(t, messageID, stored[1].ID)
			assert.Equal(t, new("answerneedle Literal [Read: quotedneedle.txt]"), stored[1].DialogueText)
		}
		for _, mode := range []string{"substring", "regex", "fts"} {
			page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{
				SessionID: prepared.Session.ID, Pattern: "reasoningneedle", Mode: mode,
				Sources: []string{"messages"}, IncludeOneShot: true,
			})
			require.NoError(t, err)
			if reparse {
				assert.Empty(t, page.Matches, mode)
			} else {
				assert.Len(t, page.Matches, 1, mode)
			}
		}
		if reparse {
			for _, tc := range []struct{ term, source, location string }{
				{"answerneedle", "messages", "message"},
				{"quotedneedle", "messages", "message"},
				{"reasoningneedle", "thinking", "thinking"},
				{"inputneedle", "tool_input", "tool_input"},
				{"resultneedle", "tool_result", "tool_result"},
			} {
				page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: prepared.Session.ID, Pattern: tc.term, Sources: []string{tc.source}, IncludeOneShot: true})
				require.NoError(t, err)
				require.Len(t, page.Matches, 1, tc.term)
				assert.Equal(t, tc.location, page.Matches[0].Location)
			}
		}
	}
}
