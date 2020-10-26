package utils

type ErrorHandler struct {
	Err error
}

func (es *ErrorHandler) Run() error {
	return es.Err
}

type CloseErrorChannel struct {
	Error error
}

func (es *CloseErrorChannel) Close() {}

func (es *CloseErrorChannel) Err() error {
	return es.Error
}
