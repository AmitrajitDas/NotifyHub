package response

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// Response is the standard API envelope.
type Response struct {
	Success bool       `json:"success"`
	Data    any        `json:"data,omitempty"`
	Error   *ErrorBody `json:"error,omitempty"`
	Meta    *Meta      `json:"meta,omitempty"`
}

type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type Meta struct {
	Page      int    `json:"page,omitempty"`
	PerPage   int    `json:"per_page,omitempty"`
	Total     int64  `json:"total,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// JSON writes a successful response with data.
func JSON(w http.ResponseWriter, status int, data any, requestID string) {
	write(w, status, Response{
		Success: true,
		Data:    data,
		Meta:    &Meta{RequestID: requestID},
	})
}

// JSONList writes a paginated success response.
func JSONList(w http.ResponseWriter, status int, data any, page, perPage int, total int64, requestID string) {
	write(w, status, Response{
		Success: true,
		Data:    data,
		Meta:    &Meta{Page: page, PerPage: perPage, Total: total, RequestID: requestID},
	})
}

// JSONError maps an AppError to an HTTP status and writes the error envelope.
func JSONError(w http.ResponseWriter, appErr *domain.AppError, requestID string) {
	status := codeToStatus(appErr.Code)
	write(w, status, Response{
		Success: false,
		Error: &ErrorBody{
			Code:    appErr.Code,
			Message: appErr.Message,
			Details: appErr.Details,
		},
		Meta: &Meta{RequestID: requestID},
	})
}

// DecodeJSON reads and decodes the request body into dst (max 1 MB).
// Returns an AppError suitable for passing directly to JSONError.
func DecodeJSON(r *http.Request, dst any) *domain.AppError {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20) // 1 MB
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return domain.NewValidationError("request body is required", nil)
		}
		return domain.NewValidationError("invalid JSON: "+err.Error(), nil)
	}
	return nil
}

// codeToStatus maps AppError codes to HTTP status codes.
func codeToStatus(code string) int {
	switch code {
	case domain.ErrCodeNotFound:
		return http.StatusNotFound
	case domain.ErrCodeConflict:
		return http.StatusConflict
	case domain.ErrCodeValidation:
		return http.StatusUnprocessableEntity
	case domain.ErrCodeUnauthorized:
		return http.StatusUnauthorized
	case domain.ErrCodeForbidden:
		return http.StatusForbidden
	case domain.ErrCodeRateLimited:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
