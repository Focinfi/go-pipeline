package pipeline

import (
	"fmt"
)

type ErrorType string

const (
	ErrorTypeBuildHandlerFailed                   = "BuildHandlerFailed"
	ErrorTypeRefHandlerNotFound                   = "RefHandlerNotFound"
	ErrorTypeHandlerBuilderNotFound               = "HandlerBuilderNotFound"
	ErrorTypeHandleFailed                         = "HandleFailed"
	ErrorTypeHandleTimeout                        = "HandleTimeout"
	ErrorTypePipeConfTimeoutLessThanOrEqualToZero = "PipeConfTimeoutLessThanOrEqualToZero"
	ErrorTypePipeConfNonRequiredNilDefaultData    = "PipeConfNonRequiredNilDefaultData"
)

type Error struct {
	Type    ErrorType `json:"type"`
	Err     error     `json:"err"`
	Message string    `json:"message"`
}

func (err Error) Error() string {
	return err.Message
}

func (err Error) Is(errorType ErrorType) bool {
	return err.Type == errorType
}

func (err Error) OriginalErr() error {
	return err.Err
}

var (
	ErrPipeConfTimeoutLessThanOrEqualToZero = Error{
		Type:    ErrorTypePipeConfTimeoutLessThanOrEqualToZero,
		Message: "timeout less than or equal to 0",
	}
	ErrPipeConfNonRequiredNilDefaultData = Error{
		Type:    ErrorTypePipeConfNonRequiredNilDefaultData,
		Message: "non-required pipe need default data",
	}
)

func ErrRefHandlerNotFound(id string) Error {
	return Error{
		Type:    ErrorTypeRefHandlerNotFound,
		Message: fmt.Sprintf("ref handler by id(%#v) not found", id),
	}
}

func ErrHandlerBuilderNotFound(name string) Error {
	return Error{
		Type:    ErrorTypeHandlerBuilderNotFound,
		Message: fmt.Sprintf("handler builder by name(%#v) not found", name),
	}
}

func ErrHandleTimeout(desc string, ms int) Error {
	return Error{
		Type:    ErrorTypeHandleTimeout,
		Message: fmt.Sprintf("pipe(%#v) timeout within %dms", desc, ms),
	}
}

func ErrHandleFailed(desc string, err error) Error {
	return Error{
		Type:    ErrorTypeHandleFailed,
		Err:     err,
		Message: fmt.Sprintf("pipe(%#v) handle failed: %v", desc, err),
	}
}

func ErrBuildHandlerFailed(name string, err error) Error {
	return Error{
		Type:    ErrorTypeBuildHandlerFailed,
		Err:     err,
		Message: fmt.Sprintf("builder(%#v) build failed: %v", name, err),
	}
}
