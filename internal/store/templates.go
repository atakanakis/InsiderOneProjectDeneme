package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/atakanakis/notification-engine/internal/domain"
)

const templateColumns = `id, name, channel, body, created_at`

func scanTemplate(row pgx.Row) (domain.Template, error) {
	var t domain.Template
	var ch string
	err := row.Scan(&t.ID, &t.Name, &ch, &t.Body, &t.CreatedAt)
	t.Channel = domain.Channel(ch)
	return t, err
}

func (s *Store) CreateTemplate(ctx context.Context, t domain.Template) (domain.Template, error) {
	created, err := scanTemplate(s.pool.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, body) VALUES ($1, $2, $3, $4)
		RETURNING `+templateColumns, t.ID, t.Name, string(t.Channel), t.Body))
	if isUniqueViolation(err) {
		return created, ErrDuplicate
	}
	return created, err
}

func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx,
		`SELECT `+templateColumns+` FROM templates WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

func (s *Store) GetTemplateByName(ctx context.Context, name string) (domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx,
		`SELECT `+templateColumns+` FROM templates WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

// TemplateSummary is a template without its body, for listings: bodies can
// be 100 KB each, and a list of full templates grew without bound.
type TemplateSummary struct {
	ID        uuid.UUID      `json:"id"`
	Name      string         `json:"name"`
	Channel   domain.Channel `json:"channel"`
	CreatedAt time.Time      `json:"createdAt"`
}

// ListTemplates returns up to limit summaries ordered by name, starting
// after the given name (keyset pagination; names are unique).
func (s *Store) ListTemplates(ctx context.Context, after string, limit int) ([]TemplateSummary, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, channel, created_at FROM templates WHERE name > $1 ORDER BY name LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TemplateSummary{}
	for rows.Next() {
		var t TemplateSummary
		var ch string
		if err := rows.Scan(&t.ID, &t.Name, &ch, &t.CreatedAt); err != nil {
			return nil, err
		}
		t.Channel = domain.Channel(ch)
		out = append(out, t)
	}
	return out, rows.Err()
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// prefixed qualifies every column in a comma separated list with a table
// alias, needed in UPDATE ... FROM where column names would be ambiguous.
func prefixed(alias, columns string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}
