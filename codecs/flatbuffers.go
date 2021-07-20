package codecs

import (
	"errors"
	"github.com/ewe-studios/sabuhp/sabu"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/nxid"
	"net/url"
	"strings"
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
}

func copyBytes(fb func(int) int8, lt int) []byte {
	var m = make([]byte, lt)
	for i := 0; i < lt; i++ {
		m[i] = byte(fb(i))
	}
	return m
}

type FlatBufferCodec struct {
	BuilderBuffer int
}

func (j FlatBufferCodec) Encode(in sabu.Message) ([]byte, error) {
	in.Parts = nil

	var builder = flatbuffers.NewBuilder(j.BuilderBuffer)

	sabu.FlatMessageStart(builder)
	sabu.FlatMessageAddSuggestedStatusCode(builder, int16(in.SuggestedStatusCode))
	sabu.FlatMessageAddPath(builder, builder.CreateString(in.Path))
	sabu.FlatMessageAddBytes(builder, builder.CreateByteVector(in.Bytes))
	sabu.FlatMessageAddContentType(builder, builder.CreateString(in.ContentType))
	sabu.FlatMessageAddFileName(builder, builder.CreateString(in.FileName))
	sabu.FlatMessageAddFormName(builder, builder.CreateString(in.FormName))

	sabu.FlatMessageStartCookiesVector(builder, len(in.Cookies))
	for _, cookie := range in.Cookies {
		sabu.KVStart(builder)
		sabu.KVAddName(builder, builder.CreateString(cookie.Name))
		sabu.KVAddValue(builder, builder.CreateString(cookie.Value))
		sabu.FlatMessageAddCookies(builder, sabu.KVEnd(builder))
	}

	sabu.FlatMessageAddEndPartId(builder, builder.CreateString(in.Id.String()))
	if in.ExpectReply {
		sabu.FlatMessageAddExpectedReply(builder, 1)
	} else {
		sabu.FlatMessageAddExpectedReply(builder, 0)
	}

	sabu.FlatMessageAddFromAddr(builder, builder.CreateString(in.FromAddr))
	sabu.FlatMessageAddId(builder,  builder.CreateString(in.Id.String()))
	sabu.FlatMessageAddIp(builder,  builder.CreateString(in.IP))
	sabu.FlatMessageAddLocalIp(builder,  builder.CreateString(in.LocalIP))
	sabu.FlatMessageAddPartId(builder,  builder.CreateString(in.PartId.String()))
	sabu.FlatMessageAddEndPartId(builder,  builder.CreateString(in.EndPartId.String()))
	sabu.FlatMessageAddTopic(builder,  builder.CreateString(in.Topic.String()))
	sabu.FlatMessageAddSubscribeTo(builder, builder.CreateString(in.SubscribeTo))
	sabu.FlatMessageAddSubscribeGroup(builder, builder.CreateString(in.SubscribeGroup))

	if in.ReplyErr != nil {
		sabu.FlatMessageAddReplyErr(builder, builder.CreateString(in.ReplyErr.Error()))
	}


	sabu.FlatMessageStartMetadataVector(builder, len(in.Cookies))
	for headerName, headerValue := range in.Headers {
		sabu.KVStart(builder)
		sabu.KVAddName(builder, builder.CreateString(headerName))
		sabu.KVAddValue(builder, builder.CreateString(strings.Join(headerValue, ";")))
		sabu.FlatMessageAddMetadata(builder, sabu.KVEnd(builder))
	}

	sabu.FlatMessageStartHeadersVector(builder, len(in.Cookies))
	for headerName, headerValue := range in.Metadata {
		sabu.KVStart(builder)
		sabu.KVAddName(builder, builder.CreateString(headerName))
		sabu.KVAddValue(builder, builder.CreateString(headerValue))
		sabu.FlatMessageAddHeaders(builder, sabu.KVEnd(builder))
	}


	sabu.FlatMessageStartParamsVector(builder, len(in.Cookies))
	for headerName, headerValue := range in.Params {
		sabu.KVStart(builder)
		sabu.KVAddName(builder, builder.CreateString(headerName))
		sabu.KVAddValue(builder, builder.CreateString(headerValue))
		sabu.FlatMessageAddParams(builder, sabu.KVEnd(builder))
	}

	sabu.FlatMessageAddForm(builder, builder.CreateString(in.Form.Encode()))
	sabu.FlatMessageAddQuery(builder, builder.CreateString(in.Query.Encode()))
	sabu.FlatMessageAddWithin(builder, float32(in.Within / time.Millisecond))
	sabu.FlatMessageAddReplyGroup(builder, builder.CreateString(in.ReplyGroup))
	sabu.FlatMessageEnd(builder)

	return builder.FinishedBytes(), nil
}

func (j FlatBufferCodec) Decode(b []byte) (sabu.Message, error) {
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

	m.Future = nil
	return m, nil
}
