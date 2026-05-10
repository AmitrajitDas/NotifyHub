package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// WSClaims are the payload fields embedded in a WebSocket short-lived JWT.
type WSClaims struct {
	TenantID    uuid.UUID `json:"tenant_id"`
	RecipientID string    `json:"recipient_id"`
	jwt.RegisteredClaims
}

// WSTokenService issues and verifies WebSocket JWTs.
type WSTokenService struct {
	secret []byte
	ttl    time.Duration
}

func NewWSTokenService(secret string, ttlSeconds int) *WSTokenService {
	return &WSTokenService{
		secret: []byte(secret),
		ttl:    time.Duration(ttlSeconds) * time.Second,
	}
}

// Issue signs a new JWT for the given tenant + recipient pair.
func (s *WSTokenService) Issue(tenantID uuid.UUID, recipientID string) (string, error) {
	now := time.Now()
	claims := WSClaims{
		TenantID:    tenantID,
		RecipientID: recipientID,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("ws jwt: sign: %w", err)
	}
	return signed, nil
}

// Verify parses and validates a WS JWT, returning its claims.
func (s *WSTokenService) Verify(tokenStr string) (*WSClaims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &WSClaims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("ws jwt: unexpected signing method: %v", t.Header["alg"])
		}
		return s.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("ws jwt: %w", err)
	}

	claims, ok := token.Claims.(*WSClaims)
	if !ok || !token.Valid {
		return nil, errors.New("ws jwt: invalid claims")
	}
	if claims.RecipientID == "" {
		return nil, errors.New("ws jwt: missing recipient_id")
	}
	return claims, nil
}
