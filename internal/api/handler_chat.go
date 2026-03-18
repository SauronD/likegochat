package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/chatpb"
)

// SendMessageRequest 定义前端发来的 JSON 结构
type SendMessageReq struct {
	ToUserID int64  `json:"to_user_id"`
	Content  string `json:"content"`
	MsgType  int32  `json:"msg_type"`
}

func (h *APIHandler) SendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. 调用 logic 层认证服务获取真实发送者ID
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

	// 2. 解析前端传来的 JSON 请求体
	var reqBody SendMessageReq
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 3. 构造 gRPC 请求对象
	rpcReq := &chatpb.SendMessageRequest{
		FromUserId: currentUserID,
		ToUserId:   reqBody.ToUserID,
		Content:    []byte(reqBody.Content), // 转换为底层要求的 bytes
		MsgType:    reqBody.MsgType,
	}

	// 4. 调用 Logic 层 gRPC 接口
	chatCtx, chatCancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer chatCancel()
	reply, err := h.ChatClient.SendMessage(chatCtx, rpcReq)
	if err != nil {
		// 返回底层的具体错误描述给前端
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 5. 将处理结果返回给前端 (HTTP 200 OK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"msg_id":    reply.MsgId,
			"timestamp": reply.Timestamp,
		},
	})
}
