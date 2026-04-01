package service

import (
	"github.com/go-playground/validator/v10"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// toValidationError converts a validator.ValidationErrors into a domain.AppError.
func toValidationError(err error) *domain.AppError {
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return domain.NewValidationError("invalid request", nil)
	}
	details := make(map[string]any, len(ve))
	for _, fe := range ve {
		details[fe.Field()] = fe.Tag()
	}
	return domain.NewValidationError("validation failed", details)
}
