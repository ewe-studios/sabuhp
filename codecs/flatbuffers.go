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

type MessageToBuilderImpl struct {
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

func copyBytes(fb func(int) int8, lt int) []byte {
	var m = make([]byte, lt)
	for i := 0; i < lt; i++ {
		m[i] = byte(fb(i))
	}
	return m
}

func (f FlatBufferToMessageImpl) Convert(b []byte) (sabu.Message, error) {
	var flm sabu.FlatMessage
	flm.Init(b, flatbuffers.GetUOffsetT(b))

	var m sabu.Message
	m.Path = string(flm.Path())
	m.IP = string(flm.Ip())
	m.LocalIP = string(flm.LocalIp())
	m.FromAddr = string(flm.FromAddr())
	m.FileName = string(flm.FileName())
	m.FormName = string(flm.FormName())
	m.ReplyGroup = string(flm.ReplyGroup())
	m.Within = time.Duration(flm.Within())
	m.ContentType = string(flm.ContentType())
	m.ExpectReply = flm.ExpectedReply() == 1
	m.Bytes = copyBytes(flm.Bytes, flm.BytesLength())
	m.SubscribeTo = string(flm.SubscribeTo())
	m.SubscribeGroup = string(flm.SubscribeGroup())
	m.SuggestedStatusCode = int(flm.SuggestedStatusCode())

	if err := m.Topic.FromString(string(flm.Topic())); err != nil {
		return m, nerror.WrapOnly(err)
	}

	var id, parseIdErr = nxid.FromString(string(flm.Id()))
	if parseIdErr != nil {
		return m, nerror.WrapOnly(parseIdErr)
	}
	m.Id = id

	if partId, parsePartIdErr := nxid.FromString(string(flm.PartId())); parsePartIdErr == nil {
		m.PartId = partId
	}

	if endId, parseEndIdErr := nxid.FromString(string(flm.EndPartId())); parseEndIdErr == nil {
		m.EndPartId = endId
	}

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

	for i := 0; i < flm.CookiesLength(); i++ {
		var headerPart = new(sabu.KV)
		flm.Cookies(headerPart, i)
		m.Cookies = append(m.Cookies, sabu.Cookie{
			Name: string(headerPart.Name()),
			Value: string(headerPart.Value()),
		})
	}

	if m.Params == nil {
		m.Params = sabu.Params{}
	}

	for i := 0; i < flm.ParamsLength(); i++ {
		var headerPart = new(sabu.KV)
		flm.Params(headerPart, i)
		m.Params.Set(string(headerPart.Name()), string(headerPart.Value()))
	}

	if m.Metadata == nil {
		m.Metadata = sabu.Params{}
	}

	for i := 0; i < flm.MetadataLength(); i++ {
		var headerPart = new(sabu.KV)
		flm.Metadata(headerPart, i)
		m.Metadata.Set(string(headerPart.Name()), string(headerPart.Value()))
	}

	if m.Headers == nil {
		m.Headers = sabu.Header{}
	}

	for i := 0; i < flm.HeadersLength(); i++ {
		var headerPart = new(sabu.KV)
		flm.Headers(headerPart, i)
		m.Headers.Set(string(headerPart.Name()), string(headerPart.Value()))
	}

	return m, nil
}

type FlatBufferCodec struct {
	MessageConverter    MessageToBuilder
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
