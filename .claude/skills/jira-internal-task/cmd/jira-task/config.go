package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func resolveBoardID(engTeam string, boardID int) (int, error) {
	if engTeam == "" {
		return 0, fmt.Errorf("eng-team is required (set --eng-team or $JIRA_ENG_TEAM)")
	}

	if boardID != 0 {
		return boardID, nil
	}

	envVar := "JIRA_" + strings.ToUpper(engTeam) + "_BOARD_ID"
	if v := os.Getenv(envVar); v != "" {
		resolvedBoardID, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s value %q: %v", envVar, v, err)
		}
		return resolvedBoardID, nil
	}

	return 0, fmt.Errorf("board-id is required (set --board-id or $%s, or -1 to skip)", envVar)
}
