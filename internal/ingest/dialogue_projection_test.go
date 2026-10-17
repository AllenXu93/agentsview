package ingest_test

import (
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
