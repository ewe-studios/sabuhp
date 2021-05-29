package codecs

import (
	"encoding/gob"
	"encoding/json"
	"io"

	"github.com/influx6/npkg/nerror"
	"github.com/vmihailenco/msgpack/v5"
)

// GenericMsgPackCodec implements the LoginCodec interface for using
// the MsgPack LoginCodec.
type GenericMsgPackCodec struct{}

// Encode encodes giving session using the internal MsgPack format.
// Returning provided data.
func (gb *GenericMsgPackCodec) Encode(w io.Writer, s interface{}) error {
	if err := msgpack.NewEncoder(w).Encode(s); err != nil {
		return nerror.Wrap(err, "Failed to encode giving session")
	}
	return nil
}

// Decode decodes giving data into provided session instance.
func (gb *GenericMsgPackCodec) Decode(r io.Reader, s interface{}) error {
	if err := msgpack.NewDecoder(r).Decode(s); err != nil {
		return nerror.WrapOnly(err)
	}
	return nil
}

// GenericJsonCodec implements the LoginCodec interface for using
// the Json LoginCodec.
type GenericJsonCodec struct{}

// Encode encodes giving session using the internal Json format.
// Returning provided data.
func (gb *GenericJsonCodec) Encode(w io.Writer, s interface{}) error {
	if err := json.NewEncoder(w).Encode(s); err != nil {
		return nerror.Wrap(err, "Failed to encode giving session")
	}
	return nil
}

// Decode decodes giving data into provided session instance.
func (gb *GenericJsonCodec) Decode(r io.Reader, s interface{}) error {
	if err := json.NewDecoder(r).Decode(s); err != nil {
		return nerror.WrapOnly(err)
	}
	return nil
}

// GenericGobCodec implements the LoginCodec interface for using
// the gob LoginCodec.
type GenericGobCodec struct{}

// Encode encodes giving session using the internal gob format.
// Returning provided data.
func (gb *GenericGobCodec) Encode(w io.Writer, s interface{}) error {
	if err := gob.NewEncoder(w).Encode(s); err != nil {
		return nerror.Wrap(err, "Failed to encode giving session")
	}
	return nil
}

// Decode decodes giving data into provided session instance.
func (gb *GenericGobCodec) Decode(r io.Reader, s interface{}) error {
	if err := gob.NewDecoder(r).Decode(s); err != nil {
		return nerror.WrapOnly(err)
	}
	return nil
}
