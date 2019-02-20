package pipeline

import (
	"context"
	"errors"
	"time"
)

type PipeType string

const (
	PipeTypeSingle   = "single"
	PipeTypeParallel = "parallel"
)

var (
	ErrRefHandlerNotFound                   = errors.New("ref handler not found")
	ErrHandlerBuilderNotFound               = errors.New("handler builder not found")
	ErrHandleTimeout                        = errors.New("handle timeout")
	ErrPipeConfTimeoutLessThanOrEqualToZero = errors.New("timeout less than or equal to 0")
	ErrPipeConfNonRequiredNilDefaultData    = errors.New("non-required pipe need default data")
)

// PipeConf used to create a new Pipe.
type PipeConf struct {
	Desc        string      `json:"desc"`
	Timeout     int         `json:"timeout"` // in millisecond
	Required    bool        `json:"required"`
	DefaultData interface{} `json:"default_data,omitempty"` // used when Pipe handling failed

	RefHandlerID string `json:"ref_handler_id"` // use a exiting Handler

	// HandlerBuilderName the name of a builder to builds a new Handler
	HandlerBuilderName string                 `json:"handler_builder_name"`
	HandlerBuilderConf map[string]interface{} `json:"handler_builder_conf"`
}

// Validate validates the PipeConf.
// The Timeout must be positive.
// The DefaultData must not be nil when Required is false.
func (pc PipeConf) Validate() error {
	if pc.Timeout <= 0 {
		return ErrPipeConfTimeoutLessThanOrEqualToZero
	}
	if !pc.Required && pc.DefaultData == nil {
		return ErrPipeConfNonRequiredNilDefaultData
	}
	return nil
}

type Pipe struct {
	Type    PipeType `json:"type"`
	Conf    PipeConf `json:"conf"`
	Handler Handler  `json:"-"`
}

func NewSinglePipes(confs []PipeConf, handlerBuilders HandlerBuilderGetter, handlers HandlerGetter) ([]Pipe, error) {
	pipes := make([]Pipe, 0, len(confs))
	for _, conf := range confs {
		if pipe, err := NewSinglePipe(conf, handlerBuilders, handlers); err != nil {
			return nil, err
		} else {
			pipes = append(pipes, *pipe)
		}
	}
	return pipes, nil
}

func NewSinglePipe(conf PipeConf, handlerBuilders HandlerBuilderGetter, handlers HandlerGetter) (*Pipe, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	pipe := &Pipe{
		Type: PipeTypeSingle,
		Conf: conf,
	}

	if conf.RefHandlerID != "" {
		if handler, ok := handlers.GetOK(conf.RefHandlerID); !ok {
			return nil, ErrRefHandlerNotFound
		} else {
			pipe.Handler = handler
			return pipe, nil
		}
	}

	builder, ok := handlerBuilders.GetOK(conf.HandlerBuilderName)
	if !ok {
		return nil, ErrHandlerBuilderNotFound
	}
	pipe.Handler = builder.Build(conf.HandlerBuilderConf)

	return pipe, nil
}

func NewParallelPipe(confs []PipeConf, handlerBuilders HandlerBuilderGetter, handlers HandlerGetter) (*Pipe, error) {
	pipe := &Pipe{
		Type: PipeTypeParallel,
	}

	handler, err := NewParallel(confs, handlerBuilders, handlers)
	if err != nil {
		return nil, err
	}
	pipe.Handler = handler

	return pipe, nil
}

// Handle implements the Handler.
// Handles the given reqRes, set timeout for single pipe, calls Handler.Handle directly for a parallel pipe.
// Returns non-nil err when timeout or failed for a pipe which pipe.Conf.Required is true,
// otherwise returns nil err and use the pipe.Conf.DefaultData.
//
func (pipe Pipe) Handle(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error) {
	if pipe.Type == PipeTypeParallel {
		return pipe.Handler.Handle(ctx, reqRes)
	}

	doneChan := make(chan struct {
		res *HandleRes
		err error
	})
	go func() {
		res, e := pipe.Handler.Handle(ctx, reqRes)
		doneChan <- struct {
			res *HandleRes
			err error
		}{res: res, err: e}
	}()

	select {
	case resp := <-doneChan:
		err = resp.err
		respRes = resp.res
	case <-time.After(time.Millisecond * time.Duration(pipe.Conf.Timeout)):
		err = ErrHandleTimeout
	}

	// assign status
	status := HandleStatusOK
	if err != nil {
		status = HandleStatusFailed
		if err == ErrHandleTimeout {
			status = HandleStatusTimeout
		}
	}

	// fatal when required and non-nil err
	if pipe.Conf.Required && err != nil {
		return &HandleRes{
			Status:  status,
			Message: err.Error(),
		}, err
	}

	// use default value when non-required and non-nil err
	if !pipe.Conf.Required && err != nil {
		return &HandleRes{
			Status:  status,
			Message: err.Error(),
			Meta:    reqRes.Meta,
			Data:    pipe.Conf.DefaultData,
		}, nil
	}

	// ok
	respRes.Status = status
	return respRes, nil
}
