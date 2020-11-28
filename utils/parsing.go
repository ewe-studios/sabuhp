package utils

import (
	"encoding/base64"
	"errors"
	"strings"
)

// ParseAuthorization returns the scheme and token of the Authorization string
// if it's valid.
func ParseAuthorization(val string) (authType string, token string, err error) {
	authSplit := strings.SplitN(val, " ", 2)
	if len(authSplit) != 2 {
		err = errors.New("invalid authorization: Expected content: `AuthType Token`")
		return
	}

	authType = strings.TrimSpace(authSplit[0])
	token = strings.TrimSpace(authSplit[1])

	return
}

// ParseTokens parses the base64 encoded token sent as part of the Authorization string,
// It expects all parts of string to be seperated with ':', returning splitted slice.
func ParseTokens(val string) ([]string, error) {
	decoded, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(decoded), ":"), nil
}
