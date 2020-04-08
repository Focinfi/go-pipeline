package pipeline

import (
	"errors"
	"fmt"
)

var (
	ErrBuildHandlerFailed                   = errors.New("build handler failed")
	ErrRefHandlerNotFound                   = errors.New("ref handler not found")
	ErrHandlerBuilderNotFound               = errors.New("handler builder not found")
	ErrHandleFailed                         = errors.New("handle failed")
	ErrHandleTimeout                        = errors.New("handle timeout")
	ErrPipeConfTimeoutLessThanOrEqualToZero = errors.New("timeout less than or equal to 0")
	ErrPipeConfNonRequiredNilDefaultData    = errors.New("non-required pipe need default data")
)

func MakeErrHandleTimeout(desc string, ms int) error {
	return fmt.Errorf("%s: %w within %dms", desc, ErrHandleTimeout, ms)
}
