package pebbleDB

import (
	"fmt"
	"os"
	"database/sql"
	"io"
	"time"

	"github.com/msrevive/nexus2/pkg/database/schema"
	"github.com/msrevive/db-migration/internal/bsoncoder"
	"github.com/google/uuid"
	"github.com/dgraph-io/badger/v4"
	"github.com/cockroachdb/pebble/v2"
	"github.com/fxamacker/cbor/v2"
)

var (
	UserPrefix = []byte("users:")
	CharPrefix = []byte("chars:")
)

type pebbleDB struct {
	db *pebble.DB
}

func New() *pebbleDB {
	return &pebbleDB{
		db: nil,
	}
}

func (b *pebbleDB) InsertChar(char schema.Character) error {
	if b.db == nil {
		return fmt.Errorf("DB object is nil!")
	}

	charData, err := cbor.Marshal(&char)
	if err != nil {
		return fmt.Errorf("cbor: failed to marshal character %v", err)
	}

	key := append(CharPrefix, []byte(char.ID.String())...)
	return d.db.Set(key, charData, pebble.NoSync)
}

func (b *pebbleDB) InsertUser(user schema.User) error {
	if b.db == nil {
		return fmt.Errorf("DB object is nil!")
	}
	
	userData, err := cbor.Marshal(&user)
	if err != nil {
		return fmt.Errorf("cbor: failed to marshal user %v", err)
	}

	key := append(UserPrefix, []byte(user.ID)...)
	return d.db.Set(key, userData, pebble.NoSync)
}

func (b *pebbleDB) Migrate(originDB string, destDB string) error {
	return nil
}