package main

import (
	"context"

	"github.com/datazip-inc/olake"
	mongodb "github.com/datazip-inc/olake/drivers/mongodb/internal"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &mongodb.Mongo{
		CDCSupport: true,
	}
	defer driver.Close(context.Background())
	olake.RegisterDriver(driver)
}
