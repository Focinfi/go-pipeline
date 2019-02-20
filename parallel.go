package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Parallel struct {
	Pipes []Pipe `json:"pipes"`
}

func NewParallel(confs []PipeConf, handlerBuilders HandlerBuilderGetter, handlers HandlerGetter) (*Parallel, error) {
	pipes, err := NewSinglePipes(confs, handlerBuilders, handlers)
	return &Parallel{Pipes: pipes}, err
}

// Handle handles the given reqRes parallelly by each Pipe fo parallel.Pipes,
// collects the response of them, return a list of HandleRes, join the error into one,
// returns non-nil error when any Pipe returns a non-nil error
func (parallel Parallel) Handle(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error) {
	var wg sync.WaitGroup
	wg.Add(len(parallel.Pipes))

	respChan := make(chan struct {
		idx int
		res *HandleRes
		err error
	}, len(parallel.Pipes))

	for i, p := range parallel.Pipes {
		go func(idx int, pipe Pipe) {
			defer wg.Done()
			res, err := pipe.Handle(ctx, reqRes)
			respChan <- struct {
				idx int
				res *HandleRes
				err error
			}{idx: idx, res: res, err: err}
		}(i, p)
	}

	wg.Wait()
	close(respChan)

	reses := make([]interface{}, len(parallel.Pipes))
	errs := make([]string, len(parallel.Pipes))
	hasErr := false
	for resp := range respChan {
		errmsg := fmt.Sprint(resp.idx+1) + ":"
		if resp.err != nil {
			hasErr = true
			errmsg += resp.err.Error()
		} else {
			errmsg += "null"
		}
		errs[resp.idx] = errmsg
		reses[resp.idx] = resp.res.Data
	}

	if hasErr {
		return &HandleRes{
			Status: HandleStatusFailed,
			Meta:   reqRes.Meta,
			Data:   reses,
		}, errors.New("errs: " + strings.Join(errs, ","))
	}

	return &HandleRes{
		Status: HandleStatusOK,
		Meta:   reqRes.Meta,
		Data:   reses,
	}, nil
}
