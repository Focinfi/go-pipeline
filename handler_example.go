package pipeline

import (
	"context"
	"errors"
	"time"
)

var errUnknown = errors.New("unknown err")

var exampleHandlerBuilderGetter MapHandlerBuilderGetter = map[string]HandlerBuilder{
	"delay":  handlerBuilderDelay,
	"failed": handlerBuilderFailed,
	"by":     handlerBuilderBy,
}

var exampleHandlerGetter MapHandlerGetter = map[string]Handler{
	"delay_1000": handlerBuilderDelay.Build(map[string]interface{}{
		"delay": time.Millisecond * 1000,
	}),
	"failed_unknown": handlerBuilderFailed.Build(map[string]interface{}{
		"err": errUnknown,
	}),
	"by_square": handlerBuilderBy.Build(map[string]interface{}{
		"handle": func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
			data := reqRes.Data.(float64) * reqRes.Data.(float64)
			return &HandleRes{
				Meta: reqRes.Meta,
				Data: data,
			}, nil
		},
	}),
	"by_cubic": handlerBuilderBy.Build(map[string]interface{}{
		"handle": func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
			data := reqRes.Data.(float64) * reqRes.Data.(float64) * reqRes.Data.(float64)
			return &HandleRes{
				Meta: reqRes.Meta,
				Data: data,
			}, nil
		},
	}),
}

var handlerBuilderDelay = HandlerBuilderFunc(func(conf map[string]interface{}) Handler {
	return HandlerFunc(func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
		time.Sleep(conf["delay"].(time.Duration))
		return reqRes, nil
	})
})

var handlerBuilderFailed = HandlerBuilderFunc(func(conf map[string]interface{}) Handler {
	return HandlerFunc(func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
		return reqRes, conf["err"].(error)
	})
})

var handlerBuilderBy = HandlerBuilderFunc(func(conf map[string]interface{}) Handler {
	return HandlerFunc(func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
		f := conf["handle"].(func(context.Context, *HandleRes) (*HandleRes, error))
		return f(ctx, reqRes)
	})
})
