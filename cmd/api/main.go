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

	client, conn, err := api.NewAuthClient(cfg.API.LogicGRPCAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	h := &api.AuthHandler{Client: client}

	mux := http.NewServeMux()
	// 注册功能：POST:username,password
	mux.HandleFunc("/api/register", h.Register)
	// 登录：POST:username,password
	mux.HandleFunc("/api/login", h.Login)
	mux.HandleFunc("/api/verify", h.Verify)

	log.Println("api http listening on", cfg.API.HTTPAddr)
	log.Fatal(http.ListenAndServe(cfg.API.HTTPAddr, mux))
}
