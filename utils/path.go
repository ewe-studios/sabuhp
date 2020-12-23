package utils

import (
	"regexp"
)

var (
	multipleSlashes = regexp.MustCompile(`/{2,}`)
	allSlashes      = regexp.MustCompile(`^/$`)
)

func ReduceMultipleSlashToOne(t string) string {
	return multipleSlashes.ReplaceAllString(t, "/")
}

func ReduceMultipleSlashTo(t string, replace string) string {
	return multipleSlashes.ReplaceAllString(t, replace)
}

func CleanOutAllSlashes(t string) string {
	return allSlashes.ReplaceAllString(t, "")
}
