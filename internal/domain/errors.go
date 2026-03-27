package domain

import "fmt"

// Error codes — used by handlers to map to HTTP status codes.
const (
	ErrCodeNotFound     = "NOT_FOUND"
	ErrCodeConflict     = "CONFLICT"
	ErrCodeValidation   = "VALIDATION_ERROR"
	ErrCodeUnauthorized = "UNAUTHORIZED"
	ErrCodeForbidden    = "FORBIDDEN"
	ErrCodeInternal     = "INTERNAL_ERROR"
)

// AppError is the unified error type passed through all layers.
// Handlers inspect Code to decide the HTTP status; Err holds the original cause.
type AppError struct {
	Code    string
	Message string
	Details map[string]any
	Err     error // original cause, for logging
}

func (e *AppError) Error() string { return e.Message }
func (e *AppError) Unwrap() error { return e.Err }

func NewNotFoundError(msg string) *AppError {
	return &AppError{Code: ErrCodeNotFound, Message: msg}
}

func NewConflictError(msg string) *AppError {
	return &AppError{Code: ErrCodeConflict, Message: msg}
}

func NewValidationError(msg string, details map[string]any) *AppError {
	return &AppError{Code: ErrCodeValidation, Message: msg, Details: details}
}

func NewUnauthorizedError(msg string) *AppError {
	return &AppError{Code: ErrCodeUnauthorized, Message: msg}
}

func NewForbiddenError(msg string) *AppError {
	return &AppError{Code: ErrCodeForbidden, Message: msg}
}

func NewInternalError(msg string, cause error) *AppError {
	return &AppError{Code: ErrCodeInternal, Message: msg, Err: fmt.Errorf("%s: %w", msg, cause)}
}
