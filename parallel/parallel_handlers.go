package parallel

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Focinfi/pipeline"
	"github.com/Focinfi/pipeline/builders"
)

// Handlers contains a list of pipeline.Option
type Handlers []pipeline.Option

// NewHandlers parses the confJSON into []pipeline.Option,
// gets Handler from handlerMap when configs a pipeline.Option as a existing Handler,
// returns a non-nil error when Handler not found in handlerMap or pipe not found in pipe/factory.all
func NewHandlers(id string, confJSON string, handlerMap map[string]pipeline.Handler) (Handlers, error) {
	// parse confJSON
	handlers := Handlers{}
	if err := json.Unmarshal([]byte(confJSON), &handlers); err != nil {
		return nil, err
	}

	// build handlers
	for i, opt := range handlers {
		// try existing handler
		if opt.ID != "" {
			h, ok := handlerMap[opt.ID]
			if !ok {
				return nil, errors.New("handler not found: id=" + opt.ID)
			}
			handlers[i].Handler = h
			continue
		}

		//	try build from a pipe
		h, err := builders.BuildHandler(id, opt.PipeName, string(opt.PipeConf))
		if err != nil {
			return nil, err
		}
		handlers[i].ID = id + "_" + opt.PipeName
		handlers[i].Handler = h
	}

	return handlers, nil
}

// Handle call handlers.Handle parallelly, ignore resp.Params,
// resp.Value will be a []interface{},
// typically ues case: independent IO handlers
func (handlers Handlers) Handle(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
	// prepare params
	var (
		wg       sync.WaitGroup
		fatalErr error
		hValChan = make(chan struct {
			idx int
			val interface{}
			err error
		})
		respData = make([]interface{}, len(handlers))
	)
	// set wait number
	wg.Add(len(handlers))

	// start goroutines to handle
	for i, h := range handlers {
		go func(index int, handler pipeline.Option) {
			if fatalErr != nil {
				return
			}

			hRespChan := make(chan struct {
				val interface{}
				err error
			})

			// do handle
			go func() {
				resp, err := handler.Handler.Handle(ctx, args)
				if err != nil {
					hRespChan <- struct {
						val interface{}
						err error
					}{err: err}
					return
				}

				hRespChan <- struct {
					val interface{}
					err error
				}{val: resp.OutValue}
			}()

			var respResult struct {
				val interface{}
				err error
			}

			// set timeout
			select {
			case <-time.After(time.Millisecond * time.Duration(handler.TimeOutMillisecond)):
				respResult = struct {
					val interface{}
					err error
				}{err: errors.New("timeout: handler_id=" + handler.ID)}
			case respResult = <-hRespChan:
			}

			// push response
			hValChan <- struct {
				idx int
				val interface{}
				err error
			}{idx: index, val: respResult.val, err: respResult.err}
			return
		}(i, h)
	}

	// receive responses of handlers
	go func() {
		for {
			resp := <-hValChan
			func() {
				defer wg.Done()
				if resp.err != nil {
					item := handlers[resp.idx]
					if item.Required {
						fatalErr = resp.err
						return
					}

					log.Printf("handle err: handler_id=%v, err=%v", item.ID, err)
					respData[resp.idx] = item.DefaultValue
					return
				}

				respData[resp.idx] = resp.val
			}()
		}
	}()

	// wait for response
	wg.Wait()

	// build response
	return &pipeline.Resp{
		OutValue: respData,
		Params:   args.Params,
	}, fatalErr
}
