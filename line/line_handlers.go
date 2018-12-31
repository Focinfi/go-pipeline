package line

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/Focinfi/pipeline"
	"github.com/Focinfi/pipeline/builders"
	"github.com/Focinfi/pipeline/parallel"
)

// Handlers contains a list of pipeline.Option
type Handlers struct {
	ID               string            `json:"id"`
	handlersConfJSON []json.RawMessage `json:"-"`
	Handlers         []pipeline.Option `json:"handlers"`
}

// NewHandlers parses the confJSON into a Handlers.
// Assume the confJSON is a json array,
// if the element is a json object, parse it as a pipeline.Option,
// if the element is a json array, parse is as a  parallel.Handlers
func NewHandlers(id string, confJSON string, handlerMap map[string]pipeline.Handler) (*Handlers, error) {
	confs := make([]json.RawMessage, 0)

	handers := &Handlers{
		ID:               id,
		handlersConfJSON: confs,
	}

	if err := json.Unmarshal([]byte(confJSON), &confs); err != nil {
		return nil, err
	}

	for _, confBytes := range confs {
		// json object
		if strings.HasPrefix(string(confBytes), "{") {
			var opt pipeline.Option
			if err := json.Unmarshal(confBytes, &opt); err != nil {
				return nil, err
			}
			// try existing handler
			if opt.ID != "" {
				h, ok := handlerMap[opt.ID]
				if !ok {
					return nil, errors.New("handler not found: ID=" + opt.ID)
				}
				opt.Handler = h
				handers.Handlers = append(handers.Handlers, opt)
				continue
			}

			//	try build from a pipe
			h, err := builders.BuildHandler(id, opt.PipeName, string(opt.PipeConf))
			if err != nil {
				return nil, err
			}
			opt.ID = id + "_" + opt.PipeName
			opt.Handler = h
			handers.Handlers = append(handers.Handlers, opt)
			continue
		}

		// json array, a parallel Handlers
		if strings.HasPrefix(string(confBytes), "[") {
			h, err := parallel.NewHandlers(id, string(confBytes), handlerMap)
			if err != nil {
				return nil, err
			}

			handers.Handlers = append(handers.Handlers, pipeline.Option{
				ID:      id,
				Handler: h,
			})
			continue
		}

		return nil, errors.New("wrong format json")
	}

	return handers, nil
}

// Handle handles the args, call the handles.Handlers one by one
// returns non-nil error when one required handler is timeout or returns non-nil error
func (handlers *Handlers) Handle(ctx context.Context, args pipeline.Args) (*pipeline.Resp, error) {
	var resp *pipeline.Resp
	for step, h := range handlers.Handlers {
		inArgs := args
		var err error

		if h.TimeOutMillisecond <= 0 {
			resp, err = h.Handler.Handle(ctx, args)
		} else {
			resp, err = handlers.stepWithTimeout(ctx, h, args)
		}

		if err != nil {
			if h.Required {
				return nil, err
			}

			log.Printf("line-handler failed: id=%v, step=%v, err=%v", handlers.ID, step, err)
			resp = &pipeline.Resp{
				OutValue: h.DefaultValue,
				Params:   inArgs.Params,
			}
			args = pipeline.Args{
				InValue: h.DefaultValue,
				Params:  inArgs.Params,
			}
			continue
		}

		args = pipeline.Args{
			InValue: resp.OutValue,
			Params:  resp.Params,
		}
	}

	return resp, nil
}

// HandleVerbosely run all handlers and log its result
func (handlers *Handlers) HandleVerbosely(ctx context.Context, args pipeline.Args) (*pipeline.Resp, []*pipeline.RespLog, error) {
	var resp *pipeline.Resp
	logs := make([]*pipeline.RespLog, 0, len(handlers.Handlers))
	for _, h := range handlers.Handlers {
		inArgs := args
		var err error

		if h.TimeOutMillisecond <= 0 {
			resp, err = h.Handler.Handle(ctx, args)
		} else {
			resp, err = handlers.stepWithTimeout(ctx, h, args)
		}

		// returns when fatal error
		if err != nil && h.Required {
			// append to log
			outJSONB, _ := json.Marshal(resp)
			logs = append(logs, &pipeline.RespLog{
				Out: string(outJSONB),
				Err: err,
			})
			return nil, logs, err
		}

		// set default resp when non-required handler failed
		if err != nil && !h.Required {
			resp = &pipeline.Resp{
				OutValue: h.DefaultValue,
				Params:   inArgs.Params,
			}
		}

		// append to log
		outJSONB, _ := json.Marshal(resp)
		logs = append(logs, &pipeline.RespLog{
			Out: string(outJSONB),
			Err: err,
		})

		// set args for next handler
		args = pipeline.Args{
			InValue: resp.OutValue,
			Params:  resp.Params,
		}
	}

	return resp, logs, nil
}

// stepWithTimeout handles the args with timeout
func (handlers *Handlers) stepWithTimeout(ctx context.Context, h pipeline.Option, args pipeline.Args) (*pipeline.Resp, error) {
	hValChan := make(chan struct {
		resp *pipeline.Resp
		err  error
	})

	go func() {
		resp, err := h.Handler.Handle(ctx, args)
		hValChan <- struct {
			resp *pipeline.Resp
			err  error
		}{resp: resp, err: err}
	}()

	select {
	case <-time.After(time.Millisecond * time.Duration(h.TimeOutMillisecond)):
		return nil, errors.New("timeout: handler_id=" + h.ID)
	case r := <-hValChan:
		return r.resp, r.err
	}
}
