package utils

type ErrorHandler struct {
	Err error
}

func (es *ErrorHandler) Run() error {
	return es.Err
}

type CloseErrorChannel struct {
	T     string
	G     string
	Error error
}

func (es *CloseErrorChannel) Group() string {
	return es.G
}

func (es *CloseErrorChannel) Topic() string {
	return es.T
}

func (es *CloseErrorChannel) Close() {}

func (es *CloseErrorChannel) Err() error {
	return es.Error
}
