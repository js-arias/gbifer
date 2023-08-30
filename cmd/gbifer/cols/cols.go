// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package cols implements a command to manage columns
// of a GBIF occurrence table.
package cols

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: `cols [--del] [--file <file>]
	[-i|--input <file>] [-o|--output <file>]
	[<name>...]`,
	Short: "display and select columns",
	Long: `
Command cols reads a GBIF occurrence table from the standard input and selects
the indicated columns.

The arguments are the column names to be selected. If the flag --file is
defined, the indicated file will be used as the column names. Each line will
be interpreted as a column name.

A new table with the indicated columns will be printed in the standard output.
If no column names are given, the list of columns will be printed in the
standard output.

If the flag --del is given, instead of selecting the given columns, it will
remove the indicated columns.

By default, it will read the data from the standard input; use the flag
--input, or -i, to select a particular file.

By default, the results will be printed in the standard output; use the flag
--output, or -o, to define an output file.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var delFlag bool
var colFile string
var input string
var output string

func setFlags(c *command.Command) {
	c.Flags().BoolVar(&delFlag, "del", false, "")
	c.Flags().StringVar(&colFile, "file", "", "")
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

	var cols map[string]bool
	if colFile != "" {
		var err error
		cols, err = readCols(colFile)
		if err != nil {
			return err
		}
	} else if len(args) > 0 {
		cols = make(map[string]bool, len(args))
		for _, a := range args {
			a = strings.ToLower(a)
			cols[a] = true
		}
	}

	if err := readTable(in, out, cols); err != nil {
		return err
	}
	return nil
}

func readTable(r io.Reader, w io.Writer, cols map[string]bool) error {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return fmt.Errorf("when reading %q header: %v", input, err)
	}

	if len(cols) == 0 {
		for _, h := range header {
			fmt.Fprintf(w, "%s\n", h)
		}
		return nil
	}

	keep := make([]int, 0, len(header))
	if delFlag {
		for i, h := range header {
			h = strings.ToLower(h)
			if cols[h] {
				continue
			}
			keep = append(keep, i)
		}
	} else {
		for i, h := range header {
			h = strings.ToLower(h)
			if !cols[h] {
				continue
			}
			keep = append(keep, i)
		}
	}

	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	// write header
	nh := make([]string, len(keep))
	for i := range nh {
		nh[i] = header[keep[i]]
	}
	if err := out.Write(nh); err != nil {
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
			for i, f := range row {
				fmt.Printf("%s: %s\n", header[i], f)
			}
			return fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}

		nr := make([]string, len(keep))
		for i := range nr {
			nr[i] = row[keep[i]]
		}

		if err := out.Write(nr); err != nil {
			return fmt.Errorf("when writing on %q: %v", output, err)
		}
	}
	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	return nil
}

func readCols(name string) (map[string]bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("column file %q: %v", name, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	cols := make(map[string]bool)
	for i := 1; ; i++ {
		ln, err := r.ReadString('\n')
		if err != nil && len(ln) == 0 {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("on file %q: line %d: %v", name, i, err)
		}
		ln = strings.TrimSpace(ln)
		if len(ln) == 0 {
			continue
		}
		cols[strings.ToLower(ln)] = true
	}
	return cols, nil
}
