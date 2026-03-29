package api

import (
	"context"
	"encoding/json"
	"fmt"
	"likegochat/internal/common/proto/authpb"
	"net/http"
	"time"
)

type AddGroupRequest struct {
	GroupID   int64  `json:"group_id"`
	SessionID string `json:"session_id"`
}
type CreateGroupRequest struct {
	SessionID string `json:"session_id"`
	GroupName string `json:"group_name"`
}

func (h *APIHandler) AddGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method allowed", http.StatusBadRequest)
		return
	}
	in := &AddGroupRequest{}
	// 解码body中的json：
	err := json.NewDecoder(r.Body).Decode(in)
	if err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// 验证用户身份：
	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()
	verifyReply, err := h.AuthClient.Verify(ctx, &authpb.VerifyRequest{SessionId: in.SessionID})
	if err != nil {
		http.Error(w, "invalid session", http.StatusBadRequest)
		return
	}
	// 加入群聊：
	_, err = h.AuthClient.AddGroup(r.Context(), &authpb.AddGroupRequest{GroupId: in.GroupID, UserId: verifyReply.GetUserId()})
	if err != nil {
		http.Error(w, fmt.Sprintf("add group failed:%s", err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"Sucess": true})
}

func (h *APIHandler) Creategroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	in := &CreateGroupRequest{}
	err := json.NewDecoder(r.Body).Decode(in)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	verifyCtx, verifyCancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
	defer verifyCancel()
	Verifyreply, err := h.AuthClient.Verify(verifyCtx, &authpb.VerifyRequest{SessionId: in.SessionID})
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid session:%s", err), http.StatusBadRequest)
		return
	}
	userID := Verifyreply.GetUserId()
	reply, err := h.AuthClient.CreateGroup(r.Context(), &authpb.CreateGroupRequest{GroupName: in.GroupName, UserId: userID})
	if err != nil {
		http.Error(w, "Create Group Failed:"+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"Success": true,
		"GroupId": reply.GroupId,
	})

}
