package pipeline

import (
	"context"
	"testing"
)

var testJSONConf = `
[
    {
        "ref_handler_id":"by_square",
        "timeout":20,
        "required":true
    },
    [
        {
            "ref_handler_id":"by_square",
            "timeout":20,
            "required":true
        },
        {
            "ref_handler_id":"by_cubic",
            "timeout":20,
            "required":true
        }
    ]
]
`

var testFailedJSONConf = `
[
    {
        "ref_handler_id":"by_square",
        "timeout":20,
        "required":true
    },
    [
        {
            "ref_handler_id":"by_square",
            "timeout":20,
            "required":true
        },
        {
			"desc": "slow",
            "ref_handler_id":"delay_1000",
            "timeout":200,
            "required":true
        }
    ]
]
`

func TestNewLineByJSON(t *testing.T) {
	tt := []struct {
		caseName string
		jsonConf string
		pipeLen  int
		hasErr   bool
	}{
		{
			caseName: "not a json array",
			jsonConf: `{"ref_handler_id":"by_square","timeout":1000,"required":true}`,
			hasErr:   true,
		},
		{
			caseName: "parallel conf contains a array",
			jsonConf: `[[{"ref_handler_id":"by_square","timeout":1000,"required":true},[{"ref_handler_id":"by_square","timeout":1000,"required":true}]]]`,
			hasErr:   true,
		},
		{
			caseName: "single pipe conf timeout type wrong",
			jsonConf: `[{"ref_handler_id":"by_square","timeout":"1000","required":true}]`,
			hasErr:   true,
		},
		{
			caseName: "single pipe conf ref_handler_id not found",
			jsonConf: `[{"ref_handler_id":"not_found","timeout":1000,"required":true}]`,
			hasErr:   true,
		},
		{
			caseName: "parallel pipe conf wrong",
			jsonConf: `[[{"ref_handler_id":"by_square","timeout":-1,"required":true}]]`,
			hasErr:   true,
		},
		{
			caseName: "normal",
			jsonConf: testJSONConf,
			pipeLen:  2,
			hasErr:   false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			line, err := NewLineByJSON(item.jsonConf, exampleHandlerBuilderGetter, exampleHandlerGetter)
			if item.hasErr {
				if err == nil {
					t.Error("err is nil")
				} else {
					t.Log(err)
				}
				return
			}

			if len(line.Pipes) != item.pipeLen {
				t.Errorf("pipes len: want=%v, got=%v", item.pipeLen, len(line.Pipes))
			}

		})
	}
}

func TestLine_Handle(t *testing.T) {
	tt := []struct {
		caseName string
		conf     string
		res      HandleRes
		err      error
	}{
		{
			caseName: "normal",
			conf:     testJSONConf,
			res: HandleRes{
				Status: HandleStatusOK,
				Data:   []int{16, 64},
			},
			err: nil,
		},
		{
			caseName: "normal",
			conf:     testFailedJSONConf,
			res: HandleRes{
				Status: HandleStatusTimeout,
			},
			err: MakeErrHandleTimeout("slow", 200),
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			line, err := NewLineByJSON(item.conf, exampleHandlerBuilderGetter, exampleHandlerGetter)
			if err != nil {
				t.Fatal(err)
			}

			reqRes := &HandleRes{
				Data: float64(2),
			}

			res, err := line.Handle(context.Background(), reqRes)
			if item.err != nil {
				if item.err == err {
					t.Log(err)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
			if text, ok := diff(item.res, res); !ok {
				t.Error("err diff:\n", text)
			}
		})
	}
}

func TestLine_HandleVerbosely(t *testing.T) {
	tt := []struct {
		caseName string
		conf     string
		reses    []HandleRes
		hasErr   bool
	}{
		{
			caseName: "normal",
			conf:     testJSONConf,
			reses: []HandleRes{
				{
					Status: HandleStatusOK,
					Data:   4,
				},
				{
					Status: HandleStatusOK,
					Data:   []int{16, 64},
				},
			},
			hasErr: false,
		},
		{
			caseName: "normal",
			conf:     testFailedJSONConf,
			reses: []HandleRes{
				{
					Status: HandleStatusOK,
					Data:   4,
				},
				{
					Status: HandleStatusFailed,
					Data:   []interface{}{16, nil},
				},
			},
			hasErr: true,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			line, err := NewLineByJSON(item.conf, exampleHandlerBuilderGetter, exampleHandlerGetter)
			if err != nil {
				t.Fatal(err)
			}

			reqRes := &HandleRes{
				Data: float64(2),
			}

			reses, err := line.HandleVerbosely(context.Background(), reqRes)
			if item.hasErr {
				if err == nil {
					t.Error("err is nil")
				} else {
					t.Log(err)
				}
			}
			if text, ok := diff(item.reses, reses); !ok {
				t.Error("err diff:\n", text)
			}
		})
	}
}

func BenchmarkLine_Handle(b *testing.B) {
	line, err := NewLineByJSON(testJSONConf, exampleHandlerBuilderGetter, exampleHandlerGetter)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		line.Handle(context.Background(), &HandleRes{Data: float64(2)})
	}
}
