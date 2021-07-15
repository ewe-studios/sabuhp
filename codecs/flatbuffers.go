package codecs

import (
	"errors"
	"github.com/ewe-studios/sabuhp/sabu"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/nxid"
	"net/url"
	"time"
)

var _ sabu.Codec = (*FlatBufferCodec)(nil)

type MessageToBuilder interface {
	Convert(in sabu.Message) (*flatbuffers.Builder, error)
}

type MessageToBuilderImpl struct{
	Size int
}

func (m MessageToBuilderImpl) Convert(in sabu.Message) (*flatbuffers.Builder, error) {
	var builder = flatbuffers.NewBuilder(m.Size)

	return builder, nil
}

type FlatBufferToMessage interface {
	Convert(b []byte) (sabu.Message, error)
}

type FlatBufferToMessageImpl struct{}

func (f FlatBufferToMessageImpl) Convert(b []byte) (sabu.Message, error) {
	var flm sabu.FlatMessage
	flm.Init(b, flatbuffers.GetUOffsetT(b))

	var m sabu.Message
	m.Path = string(flm.Path())
	m.IP = string(flm.Ip())
	m.LocalIP = string(flm.LocalIp())
	m.FromAddr = string(flm.FromAddr())
	m.FileName = string(flm.FileName())
	m.Within = time.Duration(flm.Within())
	m.ContentType = string(flm.ContentType())
	m.ExpectReply = flm.ExpectedReply() == 1
	m.SuggestedStatusCode = int(flm.SuggestedStatusCode())

	var id, parseIdErr = nxid.FromString(string(flm.Id()))
	if parseIdErr != nil {
		return m, nerror.WrapOnly(parseIdErr)
	}
	m.Id = id

	if len(flm.ReplyErr()) != 0 {
		m.ReplyErr = errors.New(string(flm.ReplyErr()))
	}

	if len(flm.Query()) != 0 {
		var form, err = url.ParseQuery(string(flm.Query()))
		if err != nil {
			return m, nerror.WrapOnly(err)
		}
		m.Query = form
	}

	if len(flm.Form()) != 0 {
		var form, err = url.ParseQuery(string(flm.Form()))
		if err != nil {
			return m, nerror.WrapOnly(err)
		}
		m.Form = form
	}

	m.FromAddr = string(flm.FromAddr())
	m.FromAddr = string(flm.FromAddr())
	return m, nil
}

type FlatBufferCodec struct{
	MessageConverter MessageToBuilder
	FlatBufferConverter FlatBufferToMessage
}

func (j *FlatBufferCodec) Encode(message sabu.Message) ([]byte, error) {
	message.Parts = nil
	var flatBuilder, err = j.MessageConverter.Convert(message)
	if err != nil {
		return nil, nerror.WrapOnly(err)
	}
	return flatBuilder.FinishedBytes(), nil
}

func (j *FlatBufferCodec) Decode(b []byte) (sabu.Message, error) {
	var message, err = j.FlatBufferConverter.Convert(b)
	if err != nil {
		return message, nerror.WrapOnly(err)
	}
	message.Future = nil
	return message, nil
}
