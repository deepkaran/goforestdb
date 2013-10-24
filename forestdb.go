package goforestdb

/*
#cgo LDFLAGS: -lforestdb

#include <stdlib.h>
#include <string.h>
#include "forestdb/forestdb.h"

void init_fdb_config(fdb_config *config)
{

    memset(config, 0, sizeof(fdb_config));
    config->chunksize = config->offsetsize = sizeof(uint64_t);
    config->buffercache_size = 1 * 1024 * 1024;
    config->wal_threshold = 1024;
    config->seqtree = FDB_SEQTREE_USE;
    config->flag = 0;

}
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
//	"log"
)

type Errno int

func (e Errno) Error() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

var errText = map[Errno]string{
	1: "DB Operation Failed",
	2: "Invalid args for operation",
}

type Conn struct {
	db *C.fdb_handle
}

func Open(filename string) (*Conn, error) {

	var db C.fdb_handle
	var config C.fdb_config
	C.init_fdb_config(&config)

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := C.fdb_open(&db, dbname, config)

	if rv != 0 {
		return nil, errors.New(Errno(rv).Error())
	}
/*
	if db == nil {
		return nil, errors.New("forestdb succeeded without returning a database")
	}
*/
	return &Conn{&db}, nil
}

func (c *Conn) Put(key, meta, value []byte) error {

	var k, m, v unsafe.Pointer

	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}
	
	if len(meta) != 0 {
		m = unsafe.Pointer(&meta[0])
	}
	
	if len(value) != 0 {
		v = unsafe.Pointer(&value[0])
	}
	
	lenk := len(key)
	lenm := len(meta)
	lenv := len(value)

	var doc *C.fdb_doc

	C.fdb_doc_create(&doc,
		k, C.size_t(lenk), m, C.size_t(lenm), v, C.size_t(lenv))
	defer C.fdb_doc_free(doc)

	rv := C.fdb_set(c.db, doc)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func (c *Conn) Get(key []byte) ([]byte, error) {

	var k unsafe.Pointer
	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}
	lenk := len(key)

	var doc *C.fdb_doc

	C.fdb_doc_create(&doc, k, C.size_t(lenk), nil, C.size_t(0), nil, C.size_t(0))
	defer C.fdb_doc_free(doc)
	rv := C.fdb_get(c.db, doc)

	if rv != 0 {
		return nil, errors.New(Errno(rv).Error())
	}

	value := doc.body
	vallen := doc.bodylen
	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil
}

func (c *Conn) Delete(key []byte) error {

	var k unsafe.Pointer
	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}
	
	lenk := len(key)

	var doc *C.fdb_doc

	C.fdb_doc_create(&doc, k, C.size_t(lenk), nil, C.size_t(0), nil, C.size_t(0))
	defer C.fdb_doc_free(doc)
	rv := C.fdb_set(c.db, doc)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func (c *Conn) Compact(newfilename string) error {

	f := C.CString(newfilename)
	defer C.free(unsafe.Pointer(f))

	rv := C.fdb_compact(c.db, f)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func (c *Conn) Commit() error {
	rv := C.fdb_commit(c.db)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func (c *Conn) Close() error {
	rv := C.fdb_close(c.db)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func Shutdown() error {
	rv := C.fdb_shutdown()
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}
