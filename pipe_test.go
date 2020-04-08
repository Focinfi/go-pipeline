package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestPipeConf_Validate(t *testing.T) {
	tt := []struct {
		caseName string
		pc       PipeConf
		hasErr   bool
		err      error
	}{
		{
			caseName: "empty conf",
			pc:       PipeConf{},
			hasErr:   true,
			err:      ErrPipeConfTimeoutLessThanOrEqualToZero,
		},
		{
			caseName: "negative timeout",
			pc:       PipeConf{Timeout: -1},
			hasErr:   true,
			err:      ErrPipeConfTimeoutLessThanOrEqualToZero,
		},
		{
			caseName: "empty default_data",
			pc:       PipeConf{Timeout: 1000},
			hasErr:   true,
			err:      ErrPipeConfNonRequiredNilDefaultData,
		},
		{
			caseName: "empty default_data when required is false",
			pc:       PipeConf{Timeout: 1000, Required: false},
			hasErr:   true,
			err:      ErrPipeConfNonRequiredNilDefaultData,
		},
		{
			caseName: "normal required conf",
			pc:       PipeConf{Timeout: 1000, Required: true},
			hasErr:   false,
		},
		{
			caseName: "normal non-required conf",
			pc:       PipeConf{Timeout: 1000, Required: false, DefaultData: "1"},
			hasErr:   false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			err := item.pc.Validate()
			if item.hasErr {
				if err == nil {
					t.Error("err non-nil")
				}
				if err != item.err {
					t.Errorf("err: want=%v, got=%v", item.err, err)
				}
				return
			}
		})
	}
}

func TestNewSinglePipe(t *testing.T) {
	tt := []struct {
		caseName string
		pc       PipeConf
		pipe     Pipe
		err      error
	}{
		{
			caseName: "unvalidated conf",
			pc: PipeConf{
				Desc:     "negative-timeout",
				Timeout:  -1,
				Required: false,
			},
			err: fmt.Errorf("negative-timeout: %w", ErrPipeConfTimeoutLessThanOrEqualToZero),
		},
		{
			caseName: "ref handler id not found",
			pc: PipeConf{
				Timeout:      1000,
				Required:     true,
				RefHandlerID: "not_found",
			},
			err: fmt.Errorf("not_found: %w", ErrRefHandlerNotFound),
		},
		{
			caseName: "handler builder name not found",
			pc: PipeConf{
				Timeout:            1000,
				Required:           true,
				HandlerBuilderName: "not_found",
			},
			err: fmt.Errorf("not_found: %w", ErrHandlerBuilderNotFound),
		},
		{
			caseName: "normal by handler builder",
			pc: PipeConf{
				Timeout:      1000,
				Required:     true,
				RefHandlerID: "delay_1000",
			},
			pipe: Pipe{
				Type: PipeTypeSingle,
				Conf: PipeConf{
					Timeout:      1000,
					Required:     true,
					RefHandlerID: "delay_1000",
				},
				Handler: exampleHandlerGetter["delay_1000"],
			},
		},
		{
			caseName: "normal by ref handler",
			pc: PipeConf{
				Timeout:            1000,
				Required:           true,
				HandlerBuilderName: "delay",
				HandlerBuilderConf: map[string]interface{}{
					"delay": 1000,
				},
			},
			pipe: Pipe{
				Type: PipeTypeSingle,
				Conf: PipeConf{
					Timeout:            1000,
					Required:           true,
					HandlerBuilderName: "delay",
					HandlerBuilderConf: map[string]interface{}{
						"delay": 1000,
					},
				},
				Handler: delay1000,
			},
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			pipe, err := NewSinglePipe(item.pc, exampleHandlerBuilderGetter, exampleHandlerGetter)
			if !reflect.DeepEqual(item.err, nil) {
				if !reflect.DeepEqual(err, item.err) {
					t.Errorf("err: want=%v, got=%v", item.err, err)
				}
				return
			}

			if text, ok := diff(item.pipe, pipe); !ok {
				t.Error("pipe: diff=\n", text)
			}
		})
	}

}

func TestNewSinglePipes(t *testing.T) {
	confs := []PipeConf{
		{
			Timeout:      1000,
			Required:     true,
			RefHandlerID: "delay_1000",
		},
		{
			Timeout:            1000,
			Required:           true,
			HandlerBuilderName: "delay",
			HandlerBuilderConf: map[string]interface{}{
				"delay": 1000,
			},
		},
	}

	pipes, err := NewSinglePipes(confs, exampleHandlerBuilderGetter, exampleHandlerGetter)
	if err != nil {
		t.Error(err)
	}
	if len(pipes) != len(confs) {
		t.Errorf("pipes len: want=%v, got=%v", len(confs), len(pipes))
	}
}

func TestNewParallelPipe(t *testing.T) {
	confs := []PipeConf{
		{
			Timeout:      1000,
			Required:     true,
			RefHandlerID: "delay_1000",
		},
		{
			Timeout:            1000,
			Required:           true,
			HandlerBuilderName: "delay",
			HandlerBuilderConf: map[string]interface{}{
				"delay": 1000,
			},
		},
	}

	pipe, err := NewParallelPipe(confs, exampleHandlerBuilderGetter, exampleHandlerGetter)
	if err != nil {
		t.Error(err)
	}

	if pipe.Type != PipeTypeParallel {
		t.Errorf("type: want=%v, got=%v", PipeTypeParallel, pipe.Type)
	}
	if parallel, ok := pipe.Handler.(*Parallel); !ok {
		t.Errorf("handler: want=%v, got=%v", true, ok)
	} else if len(parallel.Pipes) != len(confs) {
		t.Errorf("parallel pipes len: want=%v, got=%v", len(confs), len(parallel.Pipes))
	}
}

func TestSinglePipe_Handle(t *testing.T) {
	tt := []struct {
		caseName string
		pc       PipeConf
		res      HandleRes
		hasErr   bool
	}{
		{
			caseName: "required but timeout",
			pc: PipeConf{
				Timeout:      500,
				Required:     true,
				RefHandlerID: "delay_1000",
			},
			hasErr: true,
		},
		{
			caseName: "required but failed",
			pc: PipeConf{
				Timeout:      500,
				Required:     true,
				RefHandlerID: "failed_unknown",
			},
			hasErr: true,
		},
		{
			caseName: "non-required but timeout",
			pc: PipeConf{
				Desc:         "slow",
				Timeout:      500,
				Required:     false,
				DefaultData:  -1,
				RefHandlerID: "delay_1000",
			},
			res: HandleRes{
				Status:  HandleStatusTimeout,
				Message: MakeErrHandleTimeout("slow", 500).Error(),
				Data:    -1,
			},
			hasErr: false,
		},
		{
			caseName: "non-required but failed",
			pc: PipeConf{
				Timeout:      500,
				Required:     false,
				DefaultData:  -1,
				RefHandlerID: "failed_unknown",
			},
			res: HandleRes{
				Status:  HandleStatusFailed,
				Message: errUnknown.Error(),
				Data:    -1,
			},
			hasErr: false,
		},
		{
			caseName: "required and passed",
			pc: PipeConf{
				Timeout:      500,
				Required:     true,
				RefHandlerID: "by_square",
			},
			res: HandleRes{
				Status: HandleStatusOK,
				Data:   4,
			},
			hasErr: false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			pipe, err := NewSinglePipe(item.pc, exampleHandlerBuilderGetter, exampleHandlerGetter)
			if err != nil {
				t.Fatal(err)
			}

			reqRes := &HandleRes{Data: float64(2)}
			respRes, err := pipe.Handle(context.Background(), reqRes)
			if item.hasErr {
				if err == nil {
					t.Error("err is nil")
				} else {
					t.Log(err)
				}
				return
			}

			if text, ok := diff(item.res, respRes); !ok {
				t.Error("res diff:\n", text)
			}
		})
	}
}

func TestSinglePipe_Handle_Nil_Req(t *testing.T) {
	conf := PipeConf{
		Timeout:      20,
		Required:     true,
		RefHandlerID: "delay_10",
	}
	pipe, err := NewSinglePipe(conf, nil, exampleHandlerGetter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipe.Handle(context.Background(), nil); err != nil {
		t.Errorf("want nil error, got: %v", err)
	}
}
