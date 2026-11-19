package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/store"
)

// ErrorBody is the single error shape used by every endpoint.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Details []domain.FieldError `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, details ...domain.FieldError) {
	writeJSON(w, status, ErrorBody{Error: ErrorDetail{Code: code, Message: msg, Details: details}})
}

// writeServiceError maps domain and store errors to HTTP responses. Unknown
// errors become a 500 without leaking internals; the log has the details.
func writeServiceError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var verr *domain.ValidationError
	switch {
	case errors.As(err, &verr):
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "the request is invalid", verr.Fields...)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, store.ErrIdempotencyMismatch):
		writeError(w, http.StatusConflict, "idempotency_key_reused", err.Error())
	case errors.Is(err, store.ErrDuplicate):
		writeError(w, http.StatusConflict, "already_exists", "a resource with this name already exists")
	default:
		log.ErrorContext(r.Context(), "request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// decodeJSON reads a JSON body with a size cap and rejects unknown fields,
// so a typo like "priorty" is reported instead of silently ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large",
				fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit))
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_json", "body must contain a single JSON object")
		return false
	}
	return true
}
