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

func TestDialogueProjectionReachesStorage(t *testing.T) {
	const dialogue = "Literal [Read: quokka.txt]"
	parsed := parser.ParseResult{
		Session: parser.ParsedSession{ID: "dialogue", Agent: parser.AgentPi},
		Messages: []parser.ParsedMessage{{
			Ordinal: 0, Role: parser.RoleAssistant,
			Content: dialogue + " [Read: tool.txt]", DialogueText: new(dialogue),
		}},
	}
	candidate, err := ingest.PrepareCandidate(t.Context(), parsed, ingest.ContentOptions{})
	require.NoError(t, err)
	prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
	require.NoError(t, err)
	destination := dbtest.OpenTestDB(t)
	_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: prepared.Messages, ReplaceMessages: true}})
	require.NoError(t, err)
	stored, err := destination.GetAllMessages(t.Context(), prepared.Session.ID)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Equal(t, new(dialogue), stored[0].DialogueText)
}

func TestOpenHandsUnpairedObservationKeepsStoredContentSearch(t *testing.T) {
	root := t.TempDir()
	sessionDir := filepath.Join(root, "11111111111111111111111111111111")
	eventsDir := filepath.Join(sessionDir, "events")
	require.NoError(t, os.MkdirAll(eventsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sessionDir, "base_state.json"),
		[]byte(`{"id":"11111111-1111-1111-1111-111111111111"}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(eventsDir, "event.json"), []byte(`{
		"id":"e1", "source":"environment", "kind":"ObservationEvent",
		"observation":{"content":[{"type":"text","text":"unpairedneedle"}],"kind":"TerminalObservation"}
	}`), 0o644))
	provider, ok := parser.NewProvider(parser.AgentOpenHands, parser.ProviderConfig{Roots: []string{root}, Machine: "local"})
	require.True(t, ok)
	source, found, err := provider.FindSource(t.Context(), parser.FindSourceRequest{StoredFilePath: sessionDir})
	require.NoError(t, err)
	require.True(t, found)
	fingerprint, err := provider.Fingerprint(t.Context(), source)
	require.NoError(t, err)
	outcome, err := provider.Parse(t.Context(), parser.ParseRequest{Source: source, Fingerprint: fingerprint})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)
	candidate, err := ingest.PrepareCandidate(t.Context(), outcome.Results[0].Result, ingest.ContentOptions{})
	require.NoError(t, err)
	prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
	require.NoError(t, err)
	destination := dbtest.OpenTestDB(t)
	_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: prepared.Messages, ReplaceMessages: true}})
	require.NoError(t, err)
	stored, err := destination.GetAllMessages(t.Context(), prepared.Session.ID)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Nil(t, stored[0].DialogueText)
	assert.Equal(t, "unpairedneedle", stored[0].Content)
	count, err := destination.ToolCallCount(t.Context(), prepared.Session.ID)
	require.NoError(t, err)
	assert.Zero(t, count)
	for _, mode := range []string{"substring", "regex", "fts", "terms"} {
		t.Run(mode, func(t *testing.T) {
			page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{
				SessionID: prepared.Session.ID, Pattern: "unpairedneedle", Mode: mode,
				Sources: []string{"messages"}, IncludeOneShot: true,
			})
			require.NoError(t, err)
			require.Len(t, page.Matches, 1)
			assert.Equal(t, "message", page.Matches[0].Location)
			assert.Equal(t, "unpairedneedle", page.Matches[0].Snippet)
		})
	}
}
