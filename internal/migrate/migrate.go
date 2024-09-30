package migrate

import (
	"github.com/msrevive/nexus2/pkg/database/schema"
)

type Migrate interface {
	InsertUser(user schema.User) error
	InsertChar(char schema.Character) error

	Migrate(originDBFile string, destDBFile string) error
}