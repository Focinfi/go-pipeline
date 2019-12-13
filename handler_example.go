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

var (
	delay10, _ = handlerBuilderDelay.Build(map[string]interface{}{
		"delay": time.Millisecond * 10,
	})
	delay1000, _ = handlerBuilderDelay.Build(map[string]interface{}{
		"delay": time.Millisecond * 1000,
	})
	failedUnknown, _ = handlerBuilderFailed.Build(map[string]interface{}{
		"err": errUnknown,
	})
	bySquare, _ = handlerBuilderBy.Build(map[string]interface{}{
		"handle": func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
			data := reqRes.Data.(float64) * reqRes.Data.(float64)
			return &HandleRes{
				Meta: reqRes.Meta,
				Data: data,
			}, nil
		},
	})
	byCubic, _ = handlerBuilderBy.Build(map[string]interface{}{
		"handle": func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
			data := reqRes.Data.(float64) * reqRes.Data.(float64) * reqRes.Data.(float64)
			return &HandleRes{
				Meta: reqRes.Meta,
				Data: data,
			}, nil
		},
	})
)

var exampleHandlerGetter MapHandlerGetter = map[string]Handler{
	"delay_10":       delay10,
	"delay_1000":     delay1000,
	"failed_unknown": failedUnknown,
	"by_square":      bySquare,
	"by_cubic":       byCubic,
}

var handlerBuilderDelay = HandlerBuilderFunc(func(conf map[string]interface{}) (Handler, error) {
	return HandlerFunc(func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
		time.Sleep(conf["delay"].(time.Duration))
		return reqRes, nil
	}), nil
})

var handlerBuilderFailed = HandlerBuilderFunc(func(conf map[string]interface{}) (Handler, error) {
	return HandlerFunc(func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
		return reqRes, conf["err"].(error)
	}), nil
})

var handlerBuilderBy = HandlerBuilderFunc(func(conf map[string]interface{}) (Handler, error) {
	return HandlerFunc(func(ctx context.Context, reqRes *HandleRes) (*HandleRes, error) {
		f := conf["handle"].(func(context.Context, *HandleRes) (*HandleRes, error))
		return f(ctx, reqRes)
	}), nil
})
