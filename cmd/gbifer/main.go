// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// GBIFer is a tool to manipulate GBIF occurrence tables.
package main

import (
	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/cmd/gbifer/cols"
	"github.com/js-arias/gbifer/cmd/gbifer/filter"
	"github.com/js-arias/gbifer/cmd/gbifer/sort"
	"github.com/js-arias/gbifer/cmd/gbifer/tax"
	"github.com/js-arias/gbifer/cmd/gbifer/withsp"
)

var app = &command.Command{
	Usage: "gbifer <command> [<argument>...]",
	Short: "a tool to manipulate GBIF occurrence tables",
}

func init() {
	app.Add(cols.Command)
	app.Add(filter.Command)
	app.Add(sort.Command)
	app.Add(tax.Command)
	app.Add(withsp.Command)
}

func main() {
	app.Main()
}
