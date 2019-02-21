package pipeline

import (
	"context"
	"encoding/json"
)

type HandleStatus int

const (
	HandleStatusOK      HandleStatus = 1
	HandleStatusTimeout HandleStatus = 2
	HandleStatusFailed  HandleStatus = 3
)

// HandleRes acts as the input/output of Handler.
type HandleRes struct {
	Status  HandleStatus           `json:"status"`
	Message string                 `json:"message"`
	Meta    map[string]interface{} `json:"meta"`
	Data    interface{}            `json:"data"`
}

// Copy copy the res using json.Marshal/json.Unmarshal.
func (res *HandleRes) Copy() (*HandleRes, error) {
	var resCopy HandleRes
	bytes, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &resCopy)
	if err != nil {
		return nil, err
	}
	return &resCopy, nil
}

type Handler interface {
	Handle(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error)
}

// HandlerFunc type is an adapter to allow the use of ordinary functions as handlers.
// If f is a function with the appropriate signature, HandlerFunc(f) is a Handler that calls f.
type HandlerFunc func(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error)

// Handle calls f(ctx, reqRes)
func (f HandlerFunc) Handle(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error) {
	return f(ctx, reqRes)
}

type HandlerBuilder interface {
	Build(conf map[string]interface{}) Handler
}

// HandlerBuilderFunc type is an adapter to allow the use of ordinary functions as handler builders.
// If f is a function with the appropriate signature, HandlerBuilderFunc(f) is a HandlerBuilder that calls f.
type HandlerBuilderFunc func(conf map[string]interface{}) Handler

// Build calls f(conf).
func (f HandlerBuilderFunc) Build(conf map[string]interface{}) Handler {
	return f(conf)
}

type HandlerBuilderGetter interface {
	GetOK(id string) (HandlerBuilder, bool)
}

// MapHandlerBuilderGetter wraps a map[string]HandlerBuilder as a MapHandlerBuilderGetter.
type MapHandlerBuilderGetter map[string]HandlerBuilder

func (m MapHandlerBuilderGetter) GetOK(id string) (HandlerBuilder, bool) {
	builder, ok := m[id]
	return builder, ok
}

type HandlerGetter interface {
	GetOK(name string) (Handler, bool)
}

// MapHandlerGetter wraps a map[string]Handler as a MapHandlerGetter.
type MapHandlerGetter map[string]Handler

func (m MapHandlerGetter) GetOK(name string) (Handler, bool) {
	handler, ok := m[name]
	return handler, ok
}
