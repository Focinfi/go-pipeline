package pipeline

import (
	"context"
	"encoding/json"
	"strings"
)

type Line struct {
	Pipes []Pipe `json:"pipes"`
}

// Handle calls l.Pipes one by one, returns immediately when one Pipe.Handle returns error.
func (l Line) Handle(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error) {
	respRes = reqRes
	for _, pipe := range l.Pipes {
		respRes, err = pipe.Handle(ctx, respRes)
		if err != nil {
			return
		}
	}
	return
}

// Handle calls l.Pipes one by one, save the copy of respRes, returns immediately when one Pipe.Handle returns error.
func (l Line) HandleVerbosely(ctx context.Context, reqRes *HandleRes) (respReses []HandleRes, err error) {
	respRes := reqRes
	respReses = make([]HandleRes, 0, len(l.Pipes))

	for _, pipe := range l.Pipes {
		respRes, err = pipe.Handle(ctx, respRes)
		if copied, err := respRes.Copy(); err != nil {
			return respReses, err
		} else {
			respReses = append(respReses, *copied)
		}

		if err != nil {
			return respReses, err
		}
	}

	return respReses, nil
}

// NewLineByJSON parses the jsonConf and creates a new Line, returns the pointer of it.
// The jsonConf must be a JSON array, if the item of the array is a object, parses it with struct PipeConf,
// else parse it with []PipeConf.
// A object item will be used to create a single Pipe, a array item will be used to create a parallel Pipe.
// The given handlerBuilders will be used to find a HandlerBuilder with the HandlerBuilderName in PipeConf.
// The given handlers will be used to find a Handler with the RefHandlerID in PipeConf.
func NewLineByJSON(jsonConf string, handlerBuilders HandlerBuilderGetter, handlers HandlerGetter) (*Line, error) {
	line := &Line{Pipes: []Pipe{}}

	confs := make([]json.RawMessage, 0)
	if err := json.Unmarshal([]byte(jsonConf), &confs); err != nil {
		return nil, err
	}

	for _, conf := range confs {
		// single pipe
		if strings.HasPrefix(string(conf), "{") {
			var pc PipeConf
			if err := json.Unmarshal(conf, &pc); err != nil {
				return nil, err
			}

			pipe, err := NewSinglePipe(pc, handlerBuilders, handlers)
			if err != nil {
				return nil, err
			}

			line.Pipes = append(line.Pipes, *pipe)
			continue
		}

		// parallel pipe
		var pcs []PipeConf
		if err := json.Unmarshal(conf, &pcs); err != nil {
			return nil, err
		}
		pipe, err := NewParallelPipe(pcs, handlerBuilders, handlers)
		if err != nil {
			return nil, err
		}
		line.Pipes = append(line.Pipes, *pipe)
	}

	return line, nil
}
