package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	authpb "likegochat/internal/common/proto/authpb"
)

type AuthHandler struct {
	Client authpb.AuthServiceClient
}

func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	out, err := h.Client.Register(ctx, &authpb.RegisterRequest{
		Username: in.Username,
		Password: in.Password,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"user_id": out.UserId})
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	out, err := h.Client.Login(ctx, &authpb.LoginRequest{
		Username:  in.Username,
		Password:  in.Password,
		Ip:        r.RemoteAddr,
		UserAgent: r.UserAgent(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	writeJSON(w, map[string]any{
		"user_id":      out.UserId,
		"access_token": out.AccessToken,
		"expires_in":   out.ExpiresIn,
	})
}

func (h *AuthHandler) Me(w http.ResponseWriter, r *http.Request) {
	token := extractBearer(r.Header.Get("Authorization"))
	if token == "" {
		http.Error(w, "missing bearer token", 401)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	out, err := h.Client.Verify(ctx, &authpb.VerifyRequest{AccessToken: token})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !out.Ok {
		http.Error(w, "unauthorized: "+out.Reason, 401)
		return
	}
	writeJSON(w, map[string]any{"user_id": out.UserId})
}

func extractBearer(h string) string {
	parts := strings.SplitN(h, " ", 2)
	if len(parts) != 2 {
		return ""
	}
	if strings.ToLower(parts[0]) != "bearer" {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
