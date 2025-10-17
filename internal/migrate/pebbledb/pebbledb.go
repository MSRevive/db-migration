package pebbledb

import (
	"fmt"

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
	return b.db.Set(key, charData, pebble.NoSync)
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
	return b.db.Set(key, userData, pebble.NoSync)
}

func (b *pebbleDB) Migrate(originDB string, destDB string) error {
	fmt.Println("Opening connection to new pebbleDB database")
	db, err := pebble.Open(destDB, &pebble.Options{
		FormatMajorVersion: pebble.FormatColumnarBlocks,
	})
	if err != nil {
		return fmt.Errorf("pebbleDB: unable to open database %v", err)
	}
	b.db = db
	defer b.db.Close()

	fmt.Println("Opening connection to old badgerDB database")
	oldDB, err := badger.Open(badger.DefaultOptions(originDB))
	if err != nil {
		return fmt.Errorf("badger: unable to open database %v", err)
	}
	defer oldDB.Close()

	//migrate users first.
	//load all users into memory, because i'm lazy
	fmt.Println("Loading all users into memory...")
	var users []schema.User
	if err := oldDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(UserPrefix); it.ValidForPrefix(UserPrefix); it.Next() {
			item := it.Item()

			item.Value(func(v []byte) error {
				var user *schema.User
				if err := bsoncoder.Decode(v, &user); err != nil {
					return fmt.Errorf("bson: failed to unmarshal %v", err)
				}

				users = append(users, *user)
				return nil
			})
		}

		return nil
	}); err != nil {
		return fmt.Errorf("badger: cannot view DB for users %v", err)
	}

	fmt.Println("Migrating users to new DB")
	for _, value := range users {
		fmt.Printf("Importing user %s...\n", value.ID)

		value.DeletedCharacters = nil
		if err := b.InsertUser(value); err != nil {
			return err
		}
	}
	fmt.Println("Finished user migration\n")

	//migrate characters now
	fmt.Println("Loading all characters into memory...")
	var characters []schema.Character
	if err := oldDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(CharPrefix); it.ValidForPrefix(CharPrefix); it.Next() {
			item := it.Item()

			item.Value(func(v []byte) error {
				var character *schema.Character
				if err := bsoncoder.Decode(v, &character); err != nil {
					return fmt.Errorf("bson: failed to unmarshal %v", err)
				}

				characters = append(characters, *character)
				return nil
			})
		}

		return nil
	}); err != nil {
		return fmt.Errorf("badger: cannot view DB for characters %v", err)
	}

	fmt.Println("Migrating characters to new DB")
	for _, oldChar := range characters {
		fmt.Printf("Importing character slot %d for SteamID:%s - %s\n", oldChar.Slot, oldChar.SteamID, oldChar.ID.String())
		if (oldChar.ID == uuid.Nil) || (oldChar.SteamID == "") {
			fmt.Printf("Character slot %d for SteamID:%s - %s is malformed! Skipping.\n")
			continue
		}

		if oldChar.DeletedAt != nil {
			fmt.Printf("Character slot %d for SteamID:%s - %s has been deleted! Skipping.\n")
			continue
		}

		oldChar.Versions = nil
		if err := b.InsertChar(oldChar); err != nil {
			return err
		}
	}

	return nil
}