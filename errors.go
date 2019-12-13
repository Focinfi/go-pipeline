package pipeline

import (
	"fmt"
)

type ErrorType string

const (
	ErrorTypeRefHandlerNotFound                   = "RefHandlerNotFound"
	ErrorTypeHandlerBuilderNotFound               = "HandlerBuilderNotFound"
	ErrorTypeHandleTimeout                        = "HandleTimeout"
	ErrorTypePipeConfTimeoutLessThanOrEqualToZero = "PipeConfTimeoutLessThanOrEqualToZero"
	ErrorTypePipeConfNonRequiredNilDefaultData    = "PipeConfNonRequiredNilDefaultData"
)

type Error struct {
	Type    ErrorType `json:"type"`
	Message string    `json:"message"`
}

func (err Error) Error() string {
	return err.Message
}

func (err Error) Is(errorType ErrorType) bool {
	return err.Type == errorType
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

func ErrRefHandlerNotFound(id string) error {
	return Error{
		Type:    ErrorTypeRefHandlerNotFound,
		Message: fmt.Sprintf("ref handler by id(%#v) not found", id),
	}
}

func ErrHandlerBuilderNotFound(name string) error {
	return Error{
		Type:    ErrorTypeHandlerBuilderNotFound,
		Message: fmt.Sprintf("handler builder by name(%#v) not found", name),
	}
}

func ErrHandleTimeout(desc string, ms int) error {
	return Error{
		Type:    ErrorTypeHandleTimeout,
		Message: fmt.Sprintf("timeout: pipe %#v within %dms", desc, ms),
	}
}
