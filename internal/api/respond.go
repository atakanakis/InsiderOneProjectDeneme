package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"

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
		msg := "the request is invalid"
		if verr.Omitted > 0 {
			msg = fmt.Sprintf("the request is invalid; showing the first %d problems, more were found", len(verr.Fields))
		}
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", msg, verr.Fields...)
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

// describeDecodeError turns encoding/json errors into messages about the
// client's JSON, without Go type names such as "domain.Channel".
func describeDecodeError(err error) string {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.Is(err, io.EOF):
		return "request body is empty"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "malformed JSON: unexpected end of input"
	case errors.As(err, &syntaxErr):
		return fmt.Sprintf("malformed JSON at byte %d", syntaxErr.Offset)
	case errors.As(err, &typeErr):
		field := typeErr.Field
		if field == "" {
			return fmt.Sprintf("body must be a JSON object, not %s", typeErr.Value)
		}
		return fmt.Sprintf("field %q must be %s, not %s", field, jsonKind(typeErr.Type.Kind()), typeErr.Value)
	case strings.HasPrefix(err.Error(), "parsing time"):
		return "timestamps must be RFC 3339, e.g. 2026-01-02T15:04:05Z"
	}
	return strings.TrimPrefix(err.Error(), "json: ")
}

func jsonKind(k reflect.Kind) string {
	switch k {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Slice, reflect.Array:
		return "an array"
	case reflect.Map, reflect.Struct:
		return "an object"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "a number"
	}
	return "a different type"
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
		writeError(w, http.StatusBadRequest, "invalid_json", describeDecodeError(err))
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_json", "body must contain a single JSON object")
		return false
	}
	return true
}
