package service

import "errors"

var (
	ErrInvalidRange   = errors.New("invalid time range: start must be before end")
	ErrInvalidSortKey = errors.New("invalid sort key")
	ErrInvalidLimit   = errors.New("invalid limit")
)