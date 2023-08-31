// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package add implements a command to add taxons to a taxonomy file
// using a GBIF occurrence table.
package add

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/gbif"
	"github.com/js-arias/gbifer/taxonomy"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: `add [--rank <rank>]
	[--file <file>] [-i|--input <file>]`,
	Short: "add taxons to a taxonomy",
	Long: `
Command add reads a GBIF occurrence table from the standard input and extracts
the taxonomy at species level. It prints the taxonomy as a TSV file, with the
name of the taxon, the GBIF ID, its rank, the taxonomic status, and the parent
taxon.

If the input taxon is a synonym, it will add it along with the valid name as
stored in GBIF.

By default, the taxa will be added up to the genus rank; to use another rank,
use the flag --rank with one of the following values:

	unranked
	kingdom
	phylum
	class
	order
	family
	genus
	species

By default, a new taxonomy will be created and printed in the standard output.
To add to an existing taxonomy file, or to write to a taxonomy file, use the
flag --file with the name of the taxonomy file.

By default, it will read the data from the standard input; use the flag
--input, or -i, to select a particular file.

This command requires an internet connection.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var input string
var taxFile string
var rankFlag string

func setFlags(c *command.Command) {
	c.Flags().StringVar(&rankFlag, "rank", taxonomy.Genus.String(), "")
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&taxFile, "file", "", "")
}

func run(c *command.Command, args []string) (err error) {
	in := c.Stdin()
	if input != "" {
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	} else {
		input = "stdin"
	}
	if rankFlag == "" {
		rankFlag = taxonomy.Genus.String()
	}

	var tx *taxonomy.Taxonomy
	if taxFile != "" {
		var err error
		tx, err = readTaxonomy()
		if err != nil {
			return err
		}
	} else {
		tx = taxonomy.NewTaxonomy()
	}
	gbif.Open()

	if err := readTable(in, c.Stderr(), tx); err != nil {
		return err
	}
	tx.Stage()

	out := c.Stdout()
	if taxFile != "" {
		var f *os.File
		f, err = os.Create(taxFile)
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
		taxFile = "stdout"
	}

	if err := tx.Write(out); err != nil {
		return fmt.Errorf("when writing to %q: %v", taxFile, err)
	}

	return nil
}

func readTaxonomy() (*taxonomy.Taxonomy, error) {
	f, err := os.Open(taxFile)
	if errors.Is(err, os.ErrNotExist) {
		return taxonomy.NewTaxonomy(), nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tx, err := taxonomy.Read(f)
	if err != nil {
		return nil, fmt.Errorf("on file %q: %v", taxFile, err)
	}

	minRank := tx.MinRank()
	r := taxonomy.GetRank(rankFlag)
	if minRank != taxonomy.Unranked && minRank < r {
		rankFlag = minRank.String()
	}

	return tx, nil
}

func readTable(r io.Reader, stderr io.Writer, tx *taxonomy.Taxonomy) error {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return fmt.Errorf("when reading %q header: %v", input, err)
	}

	keyCol := -1
	taxCol := -1
	spCol := -1
	for i, h := range header {
		h = strings.ToLower(h)
		if h == "specieskey" {
			keyCol = i
		}
		if h == "taxonkey" {
			taxCol = i
		}
		if h == "species" {
			spCol = i
		}
	}
	if keyCol < 0 && spCol < 0 {
		return fmt.Errorf("input data %q without %q or %q fields", input, "speciesKey", "species")
	}
	rank := taxonomy.GetRank(rankFlag)

	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}
		if keyCol >= 0 || taxCol >= 0 {
			var key string
			if keyCol >= 0 {
				key = row[keyCol]
				if key == "" {
					continue
				}
			}
			if taxCol >= 0 {
				key = row[taxCol]
			}
			if key == "" {
				continue
			}

			id, err := strconv.ParseInt(key, 10, 64)
			if err != nil {
				return fmt.Errorf("table %q: row %d: %v", input, ln, err)
			}
			if err := tx.AddFromGBIF(id, rank); err != nil {
				return err
			}
			continue
		}
		name := strings.Join(strings.Fields(row[spCol]), " ")
		if name == "" {
			continue
		}
		if err := tx.AddNameFromGBIF(name, rank); err != nil {
			var ambErr *taxonomy.ErrAmbiguous
			if errors.As(err, &ambErr) {
				fmt.Fprintf(stderr, "# ambiguous taxon name %q\n", taxonomy.Canon(name))
				for _, v := range ambErr.IDs {
					fmt.Fprintf(stderr, "# \t%d\n", v)
				}
				continue
			}
			return err
		}
	}

	return nil
}
