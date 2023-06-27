package hopper

import (
	"encoding/json"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

const (
	defaultDBName = "default"
	ext           = "hopper"
)

type Map map[string]any

type Hopper struct {
	*Options
	db *bbolt.DB
}

func New(options ...OptFunc) (*Hopper, error) {
	opts := &Options{
		DBName: defaultDBName,
	}
	for _, fn := range options {
		fn(opts)
	}
	dbname := fmt.Sprintf("%s.%s", opts.DBName, ext)
	db, err := bbolt.Open(dbname, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &Hopper{
		db:      db,
		Options: opts,
	}, nil
}

func (h *Hopper) DropDatabase(name string) error {
	dbname := fmt.Sprintf("%s.%s", name, ext)
	return os.Remove(dbname)
}

func (h *Hopper) CreateCollection(name string) (*bbolt.Bucket, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}
	return bucket, err
}

func (h *Hopper) Insert(collName string, data Map) (uint64, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	collBucket, err := tx.CreateBucketIfNotExists([]byte(collName))
	if err != nil {
		return 0, err
	}
	id, err := collBucket.NextSequence()
	if err != nil {
		return 0, err
	}
	b, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	if err := collBucket.Put(uint64ToBytes(id), b); err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (h *Hopper) Find(coll string, limit uint) *Filter {
	return NewFilter(h.db, coll, limit)
}

func (h *Hopper) PrintCollection(coll string) error {
	tx, err := h.db.Begin(false)
	if err != nil {
		return err
	}
	b := tx.Bucket([]byte(coll))
	if b == nil {
		return fmt.Errorf("collection (%s) not found", coll)
	}
	if err = b.ForEach(func(k, v []byte) error {
		if k == nil {
			return nil
		}
		data, err := UnmarshalKV(k, v)
		if err != nil {
			return err
		}
		fmt.Println(data)
		return nil
	}); err != nil {
		return err
	}
	return nil
}
