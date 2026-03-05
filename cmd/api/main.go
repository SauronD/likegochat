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
	mux.HandleFunc("/register", h.Register)
	mux.HandleFunc("/login", h.Login)
	mux.HandleFunc("/me", h.Me)

	log.Println("api http listening on", cfg.API.HTTPAddr)
	log.Fatal(http.ListenAndServe(cfg.API.HTTPAddr, mux))
}
