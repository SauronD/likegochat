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

// SendMessageRequest 前端单人聊天message JSON 结构
type SendMessageReq struct {
	ToUserID int64  `json:"to_user_id"`
	Content  string `json:"content"`
	MsgType  int32  `json:"msg_type"`
}

type SendGroupMessageReq struct {
	GroupID   int64  `json:"group_id"`
	Content   string `json:"content"`
	MsgType   int32  `json:"msg_type"`
	RouteMode int32  `json:"route_mode"`
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

	// 4. 调用Logic层gRPC发送服务
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
	// 兼容两种消息发送
	if !(reqBody.RouteMode == 1 || reqBody.RouteMode == 2) {
		http.Error(w, "invalid group message type", http.StatusBadRequest)
		return
	}

	chatCtx, chatCancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer chatCancel()
	reply, err := h.ChatClient.SendGroupMessage(chatCtx, &chatpb.SendGroupMessageRequest{
		FromUserId:  currentUserID,
		GroupId:     reqBody.GroupID,
		Content:     []byte(reqBody.Content),
		MsgType:     reqBody.MsgType,
		RoutingMode: reqBody.RouteMode,
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
