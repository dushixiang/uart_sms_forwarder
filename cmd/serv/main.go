package main

import (
	"github.com/dushixiang/uart_sms_forwarder/internal"
	_ "github.com/go-orz/orz/drivers/sqlite"
)

func main() {
	internal.Run("./config.yaml")
}
