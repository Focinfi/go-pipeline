package pipeline

import "encoding/json"

type Option struct {
	// ID ref of a existing handler
	ID string `json:"id"`

	// create a handler from pipe
	PipeName string          `json:"pipe_name"`
	PipeConf json.RawMessage `json:"pipe_conf"`

	// handler conf
	TimeOutMillisecond int64       `json:"time_out_millisecond"`
	Required           bool        `json:"required"`
	DefaultValue       interface{} `json:"default_value"`

	// Handler underlying handler
	Handler Handler `json:"handler"`
}
