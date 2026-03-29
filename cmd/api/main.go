package main

import (
	"log"
	"net/http"

	"likegochat/internal/api"
	"likegochat/internal/common"
)

func main() {
	cfg, err := common.LoadConfig("configs/dev.toml")
	if err != nil {
		log.Fatal(err)
	}

	authClient, authConn, err := common.NewAuthClient(cfg.API.LogicGRPCAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer authConn.Close()

	chatClient, chatConn, err := common.NewChatClient(cfg.Logic.GRPCAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer chatConn.Close()
	h := &api.APIHandler{
		AuthClient: authClient,
		ChatClient: chatClient,
	}

	mux := http.NewServeMux()
	// 注册功能：POST:username,password
	mux.HandleFunc("/api/register", h.Register)
	// 登录：POST:username,password
	mux.HandleFunc("/api/login", h.Login)
	mux.HandleFunc("/api/verify", h.Verify)

	// 单人聊天信息发送：POST:to_user_id、content、msg_type
	mux.HandleFunc("/api/send", h.SendMessage)
	// 多人聊天信息发送：POST:group_id、content、msg_type
	mux.HandleFunc("/api/groupsend", h.SendGroupMessage)
	// 创建/加入多人群聊群
	mux.HandleFunc("/api/creategroup", h.Creategroup)
	mux.HandleFunc("/api/addgroup", h.AddGroup)
	// 房间广播消息发送
	mux.HandleFunc("/api/roomsend", h.SendRoomMessage)

	log.Println("api http listening on", cfg.API.HTTPAddr)
	log.Fatal(http.ListenAndServe(cfg.API.HTTPAddr, mux))
}
