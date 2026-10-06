package data

import (
	"context"
	"fmt"
	"time"

	"entgo.io/ent/dialect/sql"
	_ "github.com/lib/pq"
	"github.com/scorify/scorify/pkg/config"
	"github.com/scorify/scorify/pkg/ent"
	"github.com/scorify/scorify/pkg/ent/user"
	"github.com/scorify/scorify/pkg/helpers"
	"github.com/sirupsen/logrus"
)

func NewClient(ctx context.Context) (*ent.Client, error) {
	db, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			config.Postgres.Host,
			config.Postgres.Port,
			config.Postgres.User,
			config.Postgres.Password,
			config.Postgres.DB,
		),
	)
	if err != nil {
		logrus.WithError(err).Fatal("failed opening connection to postgres")
		return nil, err
	}
	// Keep the connection pool predictable on a single VM while retaining
	// enough parallelism for the bounded engine update workers.
	db.DB().SetMaxOpenConns(20)
	db.DB().SetMaxIdleConns(5)
	db.DB().SetConnMaxLifetime(5 * time.Minute)
	db.DB().SetConnMaxIdleTime(time.Minute)
	client := ent.NewClient(ent.Driver(db))

	// Run the auto migration tool.
	if err := client.Schema.Create(ctx); err != nil {
		_ = client.Close()
		logrus.WithError(err).Fatalf("failed creating schema resources")
		return nil, err
	}

	exists, err := client.User.Query().
		Where(
			user.UsernameEQ("admin"),
		).Exist(ctx)
	if err != nil {
		_ = client.Close()
		logrus.WithError(err).Fatalf("failed checking if admin user exists")
		return nil, err
	}

	if !exists {
		hashedPassword, err := helpers.HashPassword("admin")
		if err != nil {
			_ = client.Close()
			logrus.WithError(err).Fatalf("failed hashing admin password")
			return nil, err
		}

		_, err = client.User.Create().
			SetUsername("admin").
			SetPassword(hashedPassword).
			SetRole(user.RoleAdmin).
			Save(ctx)
		if err != nil {
			_ = client.Close()
			logrus.WithError(err).Warnf("failed creating admin user")
			return nil, err
		}
	}

	return client, nil
}
