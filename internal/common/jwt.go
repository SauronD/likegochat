package common

import (
	"errors"
	"strconv"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"
)

type JWTManager struct {
	Issuer    string
	Audience  string
	Secret    []byte
	AccessTTL time.Duration
}

// 我们自定义的 claims：在标准 claims 外，加一个 sid
type Claims struct {
	jwtlib.RegisteredClaims
	SessionID string `json:"sid"`
}

func (m *JWTManager) SignAccessToken(userID int64, sessionID string) (token string, expiresInSec int64, err error) {
	now := time.Now()
	exp := now.Add(m.AccessTTL)

	claims := Claims{
		RegisteredClaims: jwtlib.RegisteredClaims{
			Issuer:    m.Issuer,
			Audience:  jwtlib.ClaimStrings{m.Audience},
			Subject:   strconv.FormatInt(userID, 10),
			IssuedAt:  jwtlib.NewNumericDate(now),
			ExpiresAt: jwtlib.NewNumericDate(exp),
		},
		SessionID: sessionID,
	}

	t := jwtlib.NewWithClaims(jwtlib.SigningMethodHS256, claims)
	s, err := t.SignedString(m.Secret)
	if err != nil {
		return "", 0, err
	}
	return s, int64(m.AccessTTL.Seconds()), nil
}

func (m *JWTManager) ParseAccessToken(token string) (*Claims, error) {
	parsed, err := jwtlib.ParseWithClaims(token, &Claims{}, func(t *jwtlib.Token) (any, error) {
		if t.Method != jwtlib.SigningMethodHS256 {
			return nil, errors.New("unexpected signing method")
		}
		return m.Secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
