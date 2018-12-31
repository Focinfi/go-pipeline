package pipeline

import (
	"context"
)

// Args args for handler
type Args struct {
	InValue interface{}            `json:"in_value"`
	Params  map[string]interface{} `json:"params,omitempty"`
}

// Args response for handler
type Resp struct {
	OutValue interface{}            `json:"out_value"`
	Params   map[string]interface{} `json:"params,omitempty"`
}

// RespLog resp log
type RespLog struct {
	Out string `json:"out"`
	Err error  `json:"err,omitempty"`
}

// Handler defines some one who can handle something
type Handler interface {
	Handle(ctx context.Context, args Args) (resp *Resp, err error)
}

// HandleFunc wrap a Handle function to be a Handler
type HandleFunc func(ctx context.Context, args Args) (resp *Resp, err error)

// Handle delegates to hf
func (hf HandleFunc) Handle(ctx context.Context, args Args) (resp *Resp, err error) {
	return hf(ctx, args)
}

// HandlerBuilder build a Handler with JSON conf
type HandlerBuilder interface {
	BuildHandlerByJSON(id string, confJSON string) (Handler, error)
}

// HandlerBuildFunc wrap a BuildHandlerByJSON function to be a HandlerBuilder
type HandlerBuildFunc func(id string, confJSON string) (Handler, error)

// BuildHandlerByJSON delegates to hbf
func (hbf HandlerBuildFunc) BuildHandlerByJSON(id string, confJSON string) (Handler, error) {
	return hbf(id, confJSON)
}
