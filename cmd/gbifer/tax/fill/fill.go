// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package fill implements a command to fill taxons in a taxonomy file.
package fill

import (
	"fmt"
	"io"
	"os"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/gbif"
	"github.com/js-arias/gbifer/taxonomy"
)

var Command = &command.Command{
	Usage: `fill [--rank <rank>]
	[-i|--input <file>] [-o|--output <file>]`,
	Short: "fill a taxonomy",
	Long: `
Command fill reads a taxonomy from the standard input and fills the taxa in
the taxonomy with all the children and synonyms found in GBIF.

By default, only the taxa at or below species level. To use another rank, use
the flag --rank with one of the following values:
	
This command requires an internet connection.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var input string
var output string
var rankFlag string

func setFlags(c *command.Command) {
	c.Flags().StringVar(&rankFlag, "rank", taxonomy.Species.String(), "")
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&output, "output", "", "")
	c.Flags().StringVar(&output, "o", "", "")
}

func run(c *command.Command, args []string) (err error) {
	tx, err := readTaxonomy(c.Stdin())
	if err != nil {
		return err
	}

	if rankFlag == "" {
		rankFlag = taxonomy.Species.String()
	}

	gbif.Open()
	if err := fillTax(tx); err != nil {
		return err
	}
	tx.Stage()

	out := c.Stdout()
	if output != "" {
		var f *os.File
		f, err = os.Create(output)
		if err != nil {
			return err
		}
		defer func() {
			e := f.Close()
			if e != nil && err == nil {
				err = e
			}
		}()
		out = f
	} else {
		output = "stdout"
	}

	if err := tx.Write(out); err != nil {
		return fmt.Errorf("when writing to %q: %v", output, err)
	}

	return nil
}

func readTaxonomy(r io.Reader) (*taxonomy.Taxonomy, error) {
	if input != "" {
		f, err := os.Open(input)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	} else {
		input = "stdin"
	}

	tx, err := taxonomy.Read(r)
	if err != nil {
		return nil, fmt.Errorf("on file %q: %v", input, err)
	}
	return tx, nil
}

func fillTax(tx *taxonomy.Taxonomy) error {
	rank := taxonomy.GetRank(rankFlag)

	ids := tx.IDs()
	toAdd := make(map[int64]bool, len(ids))
	for _, id := range ids {
		toAdd[id] = true
	}
	added := make(map[int64]bool, len(ids))
	for {
		if len(toAdd) == 0 {
			break
		}
		for id := range toAdd {
			if added[id] {
				delete(toAdd, id)
				continue
			}

			r := tx.Rank(id)
			if r == taxonomy.Unranked {
				added[id] = true
				delete(toAdd, id)
				continue
			}
			if r < rank {
				added[id] = true
				delete(toAdd, id)
				continue
			}

			ls, err := children(id)
			if err != nil {
				return err
			}
			for _, sp := range ls {
				if added[sp.NubKey] {
					continue
				}
				toAdd[sp.NubKey] = true
				tx.AddSpecies(sp)
			}
			delete(toAdd, id)
			added[id] = true
		}
	}
	return nil
}

func children(id int64) ([]*gbif.Species, error) {
	ls, err := gbif.Children(id)
	if err != nil {
		return nil, err
	}

	syn, err := gbif.Synonym(id)
	if err != nil {
		return nil, err
	}
	ls = append(ls, syn...)
	return ls, nil
}
