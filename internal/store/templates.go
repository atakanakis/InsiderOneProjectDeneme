package store

import (
	"context"
	"errors"
	"strings"

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

func (s *Store) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+templateColumns+` FROM templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Template{}
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
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
