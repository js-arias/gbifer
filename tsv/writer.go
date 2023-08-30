// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package tsv

import (
	"bufio"
	"io"
)

// A Writer writes records using TSV encoding.
//
// Tab is the field delimiter.
// The Writer ends each output line with \r\n.
type Writer struct {
	// Unused,
	// for compatibility with standard library csv package
	Comma   rune
	UseCRLF bool

	w bufio.Writer
}

// NewWriter returns a new Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Comma:   '\t',
		UseCRLF: true,

		w: *bufio.NewWriter(w),
	}
}

// Error reports any error
// that has occurred during a previous Write or Flush.
func (w *Writer) Error() error {
	_, err := w.w.Write(nil)
	return err
}

// Flush writes any buffered data
// to the underlying io.Writer.
// To check if an error occurred during the Flush,
// call Error.
func (w *Writer) Flush() {
	w.w.Flush()
}

// Write writes a single TSV record to w
// along with any necessary escaping.
// A record is a slice of strings
// with each string being one field.
// Writes are buffered,
// so Flush must eventually be called
// to ensure that the record is written
// to the underlying io.Writer.
func (w *Writer) Write(record []string) error {
	for i, field := range record {
		if i > 0 {
			if _, err := w.w.WriteRune('\t'); err != nil {
				return err
			}
		}

		for _, r := range field {
			switch r {
			case '\n':
				if _, err := w.w.WriteString(`\n`); err != nil {
					return err
				}
			case '\t':
				if _, err := w.w.WriteString(`\t`); err != nil {
					return err
				}
			case '\\':
				if _, err := w.w.WriteString(`\\`); err != nil {
					return err
				}
			default:
				if _, err := w.w.WriteRune(r); err != nil {
					return err
				}
			}
		}
	}
	if _, err := w.w.WriteString("\r\n"); err != nil {
		return err
	}
	return nil
}
