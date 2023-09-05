// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package sort implements a command to sort rows
// of a GBIF occurrence table.
package sort

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/gbif"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: `sort [--species]
	[-i|--input <file>] [-o|--output <file>]`,
	Short: "sort rows by its speciesKey",
	Long: `
Command sort reads a GBIF occurrence table from the standard input and sorts
the rows by the GBIF species identifier and then by the GBIF occurrence ID.

If flag --species is defined, it will sort using the valid species name. This
option requires an internet connection.

By default, it will read the data from the standard input; use the flag
--input, or -i, to select a particular file.
	
By default, the results will be printed in the standard output; use the flag
--output, or -o, to define an output file.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var spFlag bool
var input string
var output string

func setFlags(c *command.Command) {
	c.Flags().BoolVar(&spFlag, "species", false, "")
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&output, "output", "", "")
	c.Flags().StringVar(&output, "o", "", "")
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

	data, err := readTable(in)
	if err != nil {
		return err
	}

	// sort
	if spFlag {
		if err := sortBySpecies(data); err != nil {
			return err
		}
	} else {
		slices.SortFunc(data.data, func(a, b []string) int {
			if c := cmp.Compare(a[data.spCol], b[data.spCol]); c != 0 {
				return c
			}
			return cmp.Compare(a[data.gbifCol], b[data.gbifCol])
		})
	}

	if err := writeTable(out, data); err != nil {
		return err
	}
	return nil
}

type occData struct {
	header  []string
	spCol   int
	gbifCol int
	data    [][]string
}

func readTable(r io.Reader) (*occData, error) {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return nil, fmt.Errorf("when reading %q header: %v", input, err)
	}

	spCol := -1
	gbifCol := -1
	for i, h := range header {
		h = strings.ToLower(h)
		if h == "specieskey" {
			spCol = i
		}
		if h == "gbifid" {
			gbifCol = i
		}
	}
	if spCol < 0 {
		return nil, fmt.Errorf("input data %q without %q field", input, "speciesKey")
	}
	if gbifCol < 0 {
		return nil, fmt.Errorf("input data %q without %q field", input, "gbifID")
	}

	// read data
	var data [][]string
	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return nil, fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}

		data = append(data, row)
	}

	return &occData{
		header:  header,
		spCol:   spCol,
		gbifCol: gbifCol,
		data:    data,
	}, nil
}

func sortBySpecies(data *occData) error {
	gbif.Open()

	// set the map of IDs to accepted names
	ids := make(map[string]string)
	for _, d := range data.data {
		id := d[data.spCol]
		if id == "" {
			continue
		}

		sp, ok := ids[id]
		if ok {
			continue
		}
		sp, err := searchAcceptedName(id)
		if err != nil {
			return err
		}
		ids[id] = sp
	}

	// sort
	slices.SortFunc(data.data, func(a, b []string) int {
		if c := cmp.Compare(ids[a[data.spCol]], ids[b[data.spCol]]); c != 0 {
			return c
		}
		if c := cmp.Compare(a[data.spCol], b[data.spCol]); c != 0 {
			return c
		}
		return cmp.Compare(a[data.gbifCol], b[data.gbifCol])
	})

	return nil
}

func searchAcceptedName(id string) (string, error) {
	for {
		sp, err := gbif.SpeciesID(id)
		if err != nil {
			return "", err
		}
		if sp.TaxonomicStatus == "ACCEPTED" {
			return sp.CanonicalName, nil
		}
		acceptedKey := sp.AcceptedKey
		if acceptedKey == 0 {
			acceptedKey = sp.BasionymKey
		}
		if acceptedKey == 0 {
			// invalid names without a senior synonym
			return "zzzzzzzz invalid", nil
		}

		id = strconv.FormatInt(acceptedKey, 10)
	}
}

func writeTable(w io.Writer, data *occData) error {
	// write data
	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	if err := out.Write(data.header); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	for _, d := range data.data {
		if err := out.Write(d); err != nil {
			return fmt.Errorf("when writing on %q: %v", output, err)
		}
	}
	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}

	return nil
}
