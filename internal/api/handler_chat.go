package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/chatpb"
)

// SendMessageRequest 前端单人聊天message JSON结构
type SendMessageReq struct {
	ClientMsgID int64  `json:"client_msg_id"`
	ToUserID    int64  `json:"to_user_id"`
	Content     string `json:"content"`
	MsgType     int32  `json:"msg_type"`
}

// SendMessageRequest 前端群组聊天message JSON结构
type SendGroupMessageReq struct {
	ClientMsgID int64  `json:"client_msg_id"`
	GroupID     int64  `json:"group_id"`
	Content     string `json:"content"`
	MsgType     int32  `json:"msg_type"`
}

// SendMessageRequest 前端房间聊天message JSON结构
type SendRoomMessageReq struct {
	ClientMsgID int64  `json:"client_msg_id"`
	RoomID      int64  `json:"room_id"`
	Content     string `json:"content"`
	MsgType     int32  `json:"msg_type"`
}

func (h *APIHandler) SendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 调用logic层认证服务获取真实发送者ID
	sessionID := r.Header.Get("Authorization")
	if sessionID == "" {
		http.Error(w, "missing session id", http.StatusUnauthorized)
		return
	}
	verifyCtx, verifyCancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer verifyCancel()
	verifyReply, err := h.AuthClient.Verify(verifyCtx, &authpb.VerifyRequest{SessionId: sessionID})
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	currentUserID := verifyReply.UserId

	// 解析前端的JSON请求体
	var reqBody SendMessageReq
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 构造gRPC请求对象
	rpcReq := &chatpb.SendMessageRequest{
		FromUserId: currentUserID,
		ToUserId:   reqBody.ToUserID,
		Content:    []byte(reqBody.Content), // 转换为底层要求的 bytes
		MsgType:    reqBody.MsgType,
		ClietMsgId: reqBody.ClientMsgID,
	}

	// 调用Logic层gRPC发送服务
	chatCtx, chatCancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer chatCancel()
	reply, err := h.ChatClient.SendMessage(chatCtx, rpcReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"msg_id":    reply.MsgId,
			"timestamp": reply.Timestamp,
		},
	})
	if err != nil {
		log.Println(err.Error())
	}
}
func (h *APIHandler) SendGroupMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := r.Header.Get("Authorization")
	if sessionID == "" {
		http.Error(w, "missing session id", http.StatusUnauthorized)
		return
	}
	verifyCtx, verifyCancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer verifyCancel()
	verifyReply, err := h.AuthClient.Verify(verifyCtx, &authpb.VerifyRequest{SessionId: sessionID})
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	currentUserID := verifyReply.UserId

	var reqBody SendGroupMessageReq
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	chatCtx, chatCancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer chatCancel()
	reply, err := h.ChatClient.SendGroupMessage(chatCtx, &chatpb.SendSmallGroupMessageRequest{
		FromUserId:  currentUserID,
		GroupId:     reqBody.GroupID,
		Content:     []byte(reqBody.Content),
		MsgType:     reqBody.MsgType,
		ClientMsgId: reqBody.ClientMsgID,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"msg_id":    reply.MsgId,
			"timestamp": reply.Timestamp,
		},
	})
}
func (h *APIHandler) SendRoomMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := r.Header.Get("Authorization")
	if sessionID == "" {
		http.Error(w, "missing session id", http.StatusUnauthorized)
		return
	}

	in := &SendRoomMessageReq{}
	err := json.NewDecoder(r.Body).Decode(in)
	if err != nil {
		http.Error(w, "Bad json", http.StatusBadRequest)
		return
	}

	verifyCtx, verifyCancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer verifyCancel()
	verifyReply, err := h.AuthClient.Verify(verifyCtx, &authpb.VerifyRequest{SessionId: sessionID})
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	currentUserID := verifyReply.UserId
	chatCtx, chatCancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer chatCancel()

	reply, err := h.ChatClient.SendRoomMessage(chatCtx, &chatpb.SendRoomMessageRequest{
		FromUserId: currentUserID,
		RoomId:     in.RoomID,
		Content:    []byte(in.Content),
		MsgType:    in.MsgType,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"msg_id":    reply.MsgId,
		"timestamp": reply.Timestamp,
	})

}
