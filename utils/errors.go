package utils

import "fmt"

type RequestErr struct {
	Code    int
	Message string
}

func (r *RequestErr) Error() string {
	return fmt.Sprintf("Error: %d - %s", r.Code, r.Message)
}
