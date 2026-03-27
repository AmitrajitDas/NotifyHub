package repository

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/sqlc-dev/pqtype"
)

const quietHoursLayout = "15:04"

func toNullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

func fromNullString(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	s := ns.String
	return &s
}

func toNullUUID(u *uuid.UUID) uuid.NullUUID {
	if u == nil {
		return uuid.NullUUID{}
	}
	return uuid.NullUUID{UUID: *u, Valid: true}
}

func fromNullUUID(nu uuid.NullUUID) *uuid.UUID {
	if !nu.Valid {
		return nil
	}
	u := nu.UUID
	return &u
}

func toNullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

func fromNullTime(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	t := nt.Time
	return &t
}

func toNullInt32(i *int) sql.NullInt32 {
	if i == nil {
		return sql.NullInt32{}
	}
	return sql.NullInt32{Int32: int32(*i), Valid: true}
}

func fromNullInt32(ni sql.NullInt32) *int {
	if !ni.Valid {
		return nil
	}
	i := int(ni.Int32)
	return &i
}

func toRawMessage(m map[string]any) json.RawMessage {
	if m == nil {
		return json.RawMessage("{}")
	}
	b, _ := json.Marshal(m)
	return b
}

func fromRawMessage(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

func toNullRawMessage(m map[string]any) pqtype.NullRawMessage {
	if m == nil {
		return pqtype.NullRawMessage{}
	}
	b, _ := json.Marshal(m)
	return pqtype.NullRawMessage{RawMessage: b, Valid: true}
}

func toNullTimeFromTimeStr(s *string) sql.NullTime {
	if s == nil {
		return sql.NullTime{}
	}
	t, err := time.Parse(quietHoursLayout, *s)
	if err != nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t, Valid: true}
}

func fromNullTimeToTimeStr(nt sql.NullTime) *string {
	if !nt.Valid {
		return nil
	}
	s := nt.Time.Format(quietHoursLayout)
	return &s
}

func fromNullRawMessage(nr pqtype.NullRawMessage) map[string]any {
	if !nr.Valid || len(nr.RawMessage) == 0 {
		return nil
	}
	var m map[string]any
	_ = json.Unmarshal(nr.RawMessage, &m)
	return m
}
