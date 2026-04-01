package handler

import (
	"errors"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// toAppError converts any error to *domain.AppError.
// If err is already an AppError it is returned as-is.
// If err is nil, nil is returned.
// Anything else is wrapped as an internal error.
func toAppError(err error) *domain.AppError {
	if err == nil {
		return nil
	}
	var appErr *domain.AppError
	if errors.As(err, &appErr) {
		return appErr
	}
	return domain.NewInternalError("an unexpected error occurred", err)
}
