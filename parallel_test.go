package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestParallel_Handle(t *testing.T) {
	tt := []struct {
		caseName     string
		confs        []PipeConf
		procDuration time.Duration
		res          HandleRes
		hasErr       bool
	}{
		{
			caseName: "one failed",
			confs: []PipeConf{
				{
					RefHandlerID: "by_square",
					Timeout:      20,
					Required:     true,
				},
				{
					RefHandlerID: "delay_1000",
					Timeout:      500,
					Required:     true,
				},
			},
			procDuration: time.Millisecond * 510,
			res: HandleRes{
				Status: HandleStatusFailed,
				Data:   []interface{}{4, nil},
			},
			hasErr: true,
		},
		{
			caseName: "all handled",
			confs: []PipeConf{
				{
					RefHandlerID: "by_square",
					Timeout:      20,
					Required:     true,
				},
				{
					RefHandlerID: "by_cubic",
					Timeout:      20,
					Required:     true,
				},
			},
			procDuration: time.Millisecond * 20,
			res: HandleRes{
				Status: HandleStatusOK,
				Data:   []float64{4, 8},
			},
			hasErr: false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			parallel, err := NewParallelPipe(item.confs, exampleHandlerBuilderGetter, exampleHandlerGetter)
			if err != nil {
				t.Fatal(err)
			}

			reqRes := &HandleRes{Data: float64(2)}
			startTime := time.Now()
			respRes, err := parallel.Handle(context.Background(), reqRes)
			procDuration := time.Now().Sub(startTime)
			if item.hasErr {
				if err == nil {
					t.Error("err is nil")
				} else {
					t.Log(err)
				}
			}

			if procDuration > item.procDuration {
				t.Errorf("proc duration: want<=%v, got=%v", item.procDuration, procDuration)
			}

			if text, ok := diff(item.res, respRes); !ok {
				t.Error("res diff:\n", text)
			}
		})
	}
}
