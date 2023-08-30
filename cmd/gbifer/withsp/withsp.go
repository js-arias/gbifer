// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package withsp implements a command to select rows with species
// of a GBIF occurrence table.
package withsp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: "withsp [-i|--input <file>] [-o|--output <file>]",
	Short: "select rows associated with species",
	Long: `
Command withsp reads a GBIF occurrence table from the standard input and
selects the rows in which the occurrence is associated with a taxon identified
up to species level.

By default, it will read the data from the standard input; use the flag
--input, or -i, to select a particular file.
	
By default, the results will be printed in the standard output; use the flag
--output, or -o, to define an output file.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var input string
var output string

func setFlags(c *command.Command) {
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&output, "output", "", "")
	c.Flags().StringVar(&input, "o", "", "")
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

	if err := readTable(in, out); err != nil {
		return err
	}
	return nil
}

func readTable(r io.Reader, w io.Writer) error {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return fmt.Errorf("when reading %q header: %v", input, err)
	}

	spCol := -1
	for i, h := range header {
		h = strings.ToLower(h)
		if h == "specieskey" {
			spCol = i
			break
		}
	}
	if spCol < 0 {
		return fmt.Errorf("input data %q with %q field", input, "speciesKey")
	}

	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	// write header
	if err := out.Write(header); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}

	// write data
	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}

		if strings.TrimSpace(row[spCol]) == "" {
			continue
		}
		if err := out.Write(row); err != nil {
			return fmt.Errorf("when writing on %q: %v", output, err)
		}
	}
	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	return nil
}
