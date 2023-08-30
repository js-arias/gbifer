// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// GBIFer is a tool to manipulate GBIF occurrence tables.
package main

import (
	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/cmd/gbifer/cols"
)

var app = &command.Command{
	Usage: "gbifer <command> [<argument>...]",
	Short: "a tool to manipulate GBIF occurrence tables",
}

func init() {
	app.Add(cols.Command)
}

func main() {
	app.Main()
}
