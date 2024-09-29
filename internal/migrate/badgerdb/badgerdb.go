package badger

import (
	"fmt"
	"os"
	"errors"
	"database/sql"
	"io"
	"time"

	_ "modernc.org/sqlite"
	"github.com/spf13/pflag"
	"github.com/msrevive/nexus2/pkg/database/schema"
	"github.com/msrevive/nexus2/pkg/database/bsoncoder"
	"github.com/google/uuid"
	"github.com/dgraph-io/badger"
)

type badgerDB struct {
	db *badger.DB
}

func New() *badgerDB {
	return &badgerDB{
		db: nil,
	}
}

