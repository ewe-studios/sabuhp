package sabuhp

import (
	"bytes"

	"github.com/influx6/npkg"

	"github.com/influx6/npkg/njson"
)

// CreateResponse writes giving json content into the buffer.
func CreateResponse(jsn *njson.JSON) (bytes.Buffer, error) {
	var bu bytes.Buffer
	var _, err = jsn.WriteTo(&bu)
	return bu, err
}

func CreateError(err error, message string, code int) (bytes.Buffer, error) {
	return CreateResponse(njson.JSONB(func(event npkg.Encoder) {
		event.ObjectFor("error", func(encoder npkg.ObjectEncoder) {
			encoder.Int("code", code)
			encoder.String("message", message)
			encoder.String("details", err.Error())
		})
	}))
}
