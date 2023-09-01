// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package tax is a metapackage for commands
// that dealt with taxonomy.
package tax

import (
	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/cmd/gbifer/tax/add"
	"github.com/js-arias/gbifer/cmd/gbifer/tax/fill"
	"github.com/js-arias/gbifer/cmd/gbifer/tax/match"
)

var Command = &command.Command{
	Usage: "tax <command> [<argument>...]",
	Short: "commands for taxonomy",
}

func init() {
	Command.Add(add.Command)
	Command.Add(fill.Command)
	Command.Add(match.Command)
}
