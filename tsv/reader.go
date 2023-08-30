// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package tsv implements a tsv reader.
//
// This custom package
// is used as replacement of the standard library csv package
// as GBIF tab delimited files are not compatible
// with the quotation rules used by that package.
package tsv

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Parsing errors.
var ErrFieldCount = errors.New("wrong number of fields")

// A Reader reads records from a TSV-encoded file.
//
// The Reader converts all \r\n sequences in its input to plain \n.
type Reader struct {
	// Ignored field,
	// used for compatibility
	// with standard library csv package.
	Comma rune

	fieldsPerRecord int

	r     *bufio.Reader
	line  int
	col   int
	field bytes.Buffer
}

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma: '\t',
		r:     bufio.NewReader(r),
	}
}

// FieldPos returns the line corresponding
// to the record most recently read by Read.
//
// Defined for compatibility with standard library csv package.
// The field location is ignored,
// and the column is always 0.
func (r *Reader) FieldPos(field int) (line, column int) {
	return r.line, 0
}

// Read reads one record from r.
// If the record has an unexpected number of fields,
// Read returns the record along with the error ErrFieldCount.
// If there is no data left to be read,
// Read returns nil, io.EOF
func (r *Reader) Read() (record []string, err error) {
	for {
		record, err = r.parseRecord()
		if err != nil {
			return nil, err
		}
		if len(record) > 0 {
			break
		}
	}
	if r.fieldsPerRecord == 0 {
		r.fieldsPerRecord = len(record)
	}
	if len(record) != r.fieldsPerRecord {
		return record, fmt.Errorf("%w: got %d fields, want %d", ErrFieldCount, len(record), r.fieldsPerRecord)
	}
	return record, nil
}

func (r *Reader) parseRecord() (fields []string, err error) {
	r.line++
	r.col = 0

	if _, _, err := r.r.ReadRune(); err != nil {
		return nil, err
	}
	r.r.UnreadRune()

	for {
		delim, err := r.parseField()
		if err != nil {
			return nil, err
		}
		if delim == '\n' {
			f := r.field.String()
			if len(fields) > 0 || len(f) > 0 {
				fields = append(fields, r.field.String())
			}
			return fields, nil
		}
		fields = append(fields, r.field.String())
	}
}

func (r *Reader) parseField() (delim rune, err error) {
	r.field.Reset()
	for {
		r1, err := r.readRune()
		if errors.Is(err, io.EOF) {
			if r.col > 0 {
				return '\n', nil
			}
		}
		if err != nil {
			return 0, err
		}
		if r1 == '\t' || r1 == '\n' {
			return r1, nil
		}
		if r1 == '\\' {
			r1, _, err = r.r.ReadRune()
			if errors.Is(err, io.EOF) {
				if r.col > 0 {
					return '\n', nil
				}
			}
			if err != nil {
				return 0, err
			}
			switch r1 {
			case 't':
				r.field.WriteRune('\t')
				r.col++
				continue
			case 'n':
				r.field.WriteRune('\n')
				r.col++
				continue
			case '\\':
				r.field.WriteRune('\\')
				r.col++
				continue
			default:
				r.r.UnreadRune()
			}
		}
		r.field.WriteRune(r1)
	}
}

func (r *Reader) readRune() (r1 rune, err error) {
	r1, _, err = r.r.ReadRune()
	if err != nil {
		return 0, err
	}

	if r1 == '\r' {
		r1, _, err = r.r.ReadRune()
		if err != nil {
			return 0, err
		}
		if r1 != '\n' {
			r.r.UnreadRune()
			r1 = '\r'
		}
	}
	if r1 != '\n' {
		r.col++
	}

	return r1, nil
}
