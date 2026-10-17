package ingest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/artifact"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/ingest"
	"go.kenn.io/agentsview/internal/parser"
)

func TestPiSearchSeparatesDialogueThinkingAndTools(t *testing.T) {
	const body = `{"type":"session","version":3,"id":"dialogue","timestamp":"2026-05-01T10:00:00Z"}
{"type":"message","id":"u","message":{"role":"user","content":[{"type":"text","text":"Question"}]}}
{"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"Example [Read: quokka.txt] is literal.\n[Read: quokka.txt]\n[Thinking]\nquoted thought\n[/Thinking]"},{"type":"thinking","thinking":"reasoningneedle"},{"type":"toolCall","id":"call-a","name":"read","arguments":{"file_path":"quokka.txt"}},{"type":"toolCall","id":"call-b","name":"read","arguments":{"file_path":"quokka.txt"}},{"type":"toolCall","id":"call-c","name":"bash","arguments":{"command":"echo commandneedle"}},{"type":"text","text":"Dialogueanswer."}]}}
{"type":"message","id":"r","parentId":"a","message":{"role":"toolResult","toolCallId":"call-c","toolName":"bash","content":"resultneedle"}}
{"type":"message","id":"empty","parentId":"r","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-d","name":"read","arguments":{"file_path":"other.txt"}}]}}
`
	const dialogue = "Example [Read: quokka.txt] is literal.\n[Read: quokka.txt]\n[Thinking]\nquoted thought\n[/Thinking]\nDialogueanswer."
	root := t.TempDir()
	path := filepath.Join(root, "dialogue.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	provider, ok := parser.NewProvider(parser.AgentPi, parser.ProviderConfig{Roots: []string{root}, Machine: "local"})
	require.True(t, ok)
	var opaque any = parser.JSONLSource{Root: root, Path: path}
	outcome, err := provider.Parse(t.Context(), parser.ParseRequest{Source: parser.SourceRef{Provider: parser.AgentPi, DisplayPath: path, Opaque: opaque}})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)
	require.Equal(t, new(dialogue), outcome.Results[0].Result.Messages[1].DialogueText)
	candidate, err := ingest.PrepareCandidate(t.Context(), outcome.Results[0].Result, ingest.ContentOptions{})
	require.NoError(t, err)
	prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
	require.NoError(t, err)
	destination := dbtest.OpenTestDB(t)
	archived := append([]db.Message(nil), prepared.Messages...)
	for i := range archived {
		archived[i].DialogueText = nil
		archived[i].ThinkingText = ""
	}
	_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: archived, ReplaceMessages: true}})
	require.NoError(t, err)
	before, err := destination.GetAllMessages(t.Context(), prepared.Session.ID)
	require.NoError(t, err)
	_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: prepared.Messages, ReplaceMessages: true}})
	require.NoError(t, err)
	stored, err := destination.GetAllMessages(t.Context(), prepared.Session.ID)
	require.NoError(t, err)
	require.Len(t, stored, 3)
	assert.Equal(t, before[1].ID, stored[1].ID)
	require.NotNil(t, stored[1].DialogueText)
	assert.Equal(t, dialogue, *stored[1].DialogueText)
	assert.Equal(t, "reasoningneedle", stored[1].ThinkingText)
	page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: prepared.Session.ID, Pattern: "other", Mode: "fts", Sources: []string{"messages"}, IncludeOneShot: true})
	require.NoError(t, err)
	assert.Empty(t, page.Matches)

	for _, mode := range []string{"substring", "regex", "fts", "terms"} {
		for _, term := range []string{"commandneedle", "reasoningneedle", "other"} {
			page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: prepared.Session.ID, Pattern: term, Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true})
			require.NoError(t, err)
			assert.Empty(t, page.Matches, "%s messages matched %s", mode, term)
		}
		page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: prepared.Session.ID, Pattern: "Dialogueanswer", Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true})
		require.NoError(t, err)
		require.Len(t, page.Matches, 1)
		assert.NotContains(t, page.Matches[0].Snippet, "commandneedle")
		assert.NotContains(t, page.Matches[0].Snippet, "reasoningneedle")
	}
	for _, mode := range []string{"substring", "regex"} {
		for _, tc := range []struct{ term, source, location string }{
			{"reasoningneedle", "thinking", "thinking"},
			{"commandneedle", "tool_input", "tool_input"},
			{"resultneedle", "tool_result", "tool_result"},
		} {
			for _, sources := range [][]string{nil, {tc.source}} {
				page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: prepared.Session.ID, Pattern: tc.term, Mode: mode, Sources: sources, IncludeOneShot: true})
				require.NoError(t, err)
				require.Len(t, page.Matches, 1)
				assert.Equal(t, tc.location, page.Matches[0].Location)
			}
		}
	}
	for _, mode := range []string{"fts", "terms", "semantic", "hybrid"} {
		_, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{Pattern: "reasoningneedle", Mode: mode, Sources: []string{"thinking"}})
		var inputErr *db.SearchInputError
		require.ErrorAs(t, err, &inputErr)
	}

	// v4 peers still import every turn with its stored thinking.
	target := t.TempDir()
	_, err = artifact.Sync(t.Context(), destination, artifact.SyncOptions{DataDir: t.TempDir(), Target: target, Origin: "host-a"})
	require.NoError(t, err)
	peer := dbtest.OpenTestDB(t)
	_, err = artifact.Sync(t.Context(), peer, artifact.SyncOptions{DataDir: t.TempDir(), Target: target, Origin: "host-b"})
	require.NoError(t, err)
	imported, err := peer.GetAllMessages(t.Context(), "host-a~"+prepared.Session.ID)
	require.NoError(t, err)
	require.Len(t, imported, 3)
	assert.Equal(t, stored[1].Content, imported[1].Content)
	assert.Equal(t, "reasoningneedle", imported[1].ThinkingText)
	assert.Nil(t, imported[1].DialogueText)
	page, err = peer.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: "host-a~" + prepared.Session.ID, Pattern: "reasoningneedle", IncludeOneShot: true})
	require.NoError(t, err)
	require.Len(t, page.Matches, 1)
	assert.Equal(t, "message", page.Matches[0].Location)
}

func TestSharedBlockDialoguePreservesQuotedMarker(t *testing.T) {
	for _, tc := range []struct {
		agent        parser.AgentType
		file         string
		body         string
		opaque       func(root, path string) any
		wantDialogue string
		wantThinking string
	}{
		{
			agent: parser.AgentClaude,
			file:  "shared.jsonl",
			body: `{"type":"user","uuid":"u","sessionId":"shared","timestamp":"2026-05-01T10:00:00Z","message":{"role":"user","content":"Question"}}
{"type":"assistant","uuid":"a","parentUuid":"u","sessionId":"shared","timestamp":"2026-05-01T10:00:01Z","message":{"role":"assistant","content":[{"type":"text","text":"Example [Read: quokka.txt] is literal."},{"type":"tool_use","id":"read","name":"Read","input":{"file_path":"quokka.txt"}}]}}
`,
			opaque:       func(_, path string) any { return parser.MaterializedFileSource{Path: path} },
			wantDialogue: "Example [Read: quokka.txt] is literal.",
		},
		{
			agent: parser.AgentIflow,
			file:  "session-thinking.jsonl",
			body: `{"type":"user","uuid":"u","sessionId":"thinking","timestamp":"2026-05-01T10:00:00Z","message":{"role":"user","content":"Question"}}
{"type":"assistant","uuid":"a","parentUuid":"u","sessionId":"thinking","timestamp":"2026-05-01T10:00:01Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"reasoningneedle"},{"type":"text","text":"Dialogue answer."}]}}
`,
			opaque:       func(root, path string) any { return parser.JSONLSource{Root: root, Path: path} },
			wantDialogue: "Dialogue answer.",
			wantThinking: "reasoningneedle",
		},
	} {
		t.Run(string(tc.agent), func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, tc.file)
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			provider, ok := parser.NewProvider(tc.agent, parser.ProviderConfig{Roots: []string{root}, Machine: "local"})
			require.True(t, ok)
			outcome, err := provider.Parse(t.Context(), parser.ParseRequest{Source: parser.SourceRef{Provider: tc.agent, DisplayPath: path, Opaque: tc.opaque(root, path)}})
			require.NoError(t, err)
			require.Len(t, outcome.Results, 1)
			require.Len(t, outcome.Results[0].Result.Messages, 2)
			assert.Equal(t, new(tc.wantDialogue), outcome.Results[0].Result.Messages[1].DialogueText)
			assert.Equal(t, tc.wantThinking, outcome.Results[0].Result.Messages[1].ThinkingText)
		})
	}
}

func TestOpenCodeLiteralMarkerSurvivesIngest(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{
		"message/ses_literal/msg_u.json":  `{"id":"msg_u","sessionID":"ses_literal","role":"user","time":{"created":1699999999000}}`,
		"part/msg_u/part_u.json":          `{"id":"part_u","messageID":"msg_u","sessionID":"ses_literal","type":"text","text":"Read the file"}`,
		"session/global/ses_literal.json": `{"id":"ses_literal","directory":"/workspace/project","time":{"created":1700000000000,"updated":1700000010000}}`,
		"message/ses_literal/msg_a.json":  `{"id":"msg_a","sessionID":"ses_literal","role":"assistant","time":{"created":1700000000000}}`,
		"part/msg_a/part_a.json":          `{"id":"part_a","messageID":"msg_a","sessionID":"ses_literal","type":"text","text":"Example [Read: quokka.txt] is literal."}`,
		"part/msg_a/part_b.json":          `{"id":"part_b","messageID":"msg_a","sessionID":"ses_literal","type":"tool","tool":"read","callID":"call_a","state":{"input":{"file_path":"quokka.txt"}}}`,
	} {
		path := filepath.Join(root, "storage", filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	}
	provider, ok := parser.NewProvider(parser.AgentOpenCode, parser.ProviderConfig{Roots: []string{root}, Machine: "local"})
	require.True(t, ok)
	sources, err := provider.Discover(t.Context())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	outcome, err := provider.Parse(t.Context(), parser.ParseRequest{Source: sources[0]})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)
	candidate, err := ingest.PrepareCandidate(t.Context(), outcome.Results[0].Result, ingest.ContentOptions{})
	require.NoError(t, err)
	prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
	require.NoError(t, err)
	require.Len(t, prepared.Messages, 2)
	assert.Equal(t, "Example [Read: quokka.txt] is literal.", prepared.Messages[1].Content)
	destination := dbtest.OpenTestDB(t)
	_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: prepared.Messages, ReplaceMessages: true}})
	require.NoError(t, err)
	for _, mode := range []string{"substring", "regex", "fts", "terms"} {
		page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: prepared.Session.ID, Pattern: "quokka", Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true, IncludeAutomated: true})
		require.NoError(t, err)
		require.Len(t, page.Matches, 1)
		assert.Contains(t, page.Matches[0].Snippet, "[Read: quokka.txt]")
	}
}
