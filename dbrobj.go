package dbrobj

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/gocraft/dbr"
)

// TODO: sort out some of these magical things.
// Examples:
// - pks
// - update time fields
// - create time fields
// Perhaps they can be struct tags.  It's a lot easier to just do those things than
// to implement create table statements and such, and also implementing relations
// get's crazy.  If it's kept simple, it could work.  Not sure if
// `db:"customer_id,pk"` or `dbrobj:"pk"` is better.

// Hm, what the heck do we do with the table name...  Minor detailz...

// FIXME: we might need dialect! (ouch, because it's gone by now... so sad nobody can keep a damned
// copy of that little string for the driver name around)
// Turns out we, can do e.g. c.Connection.Dialect == dialect.MySQL to detect the dialect - that works...

var TableNameMapper = CamelToSnake
var FieldNameMapper = CamelToSnake

type TableInfo struct {
	GoType         reflect.Type // underlying Go type (pointer removed)
	TableName      string       // SQL table names
	KeyNames       []string     // SQL primary key field names
	KeyAutoIncr    bool         // true if keys are auto-incremented by the database
	VersionColName string       // name of version col, empty disables optimistic locking
	// TODO: function to generate new version number (should increment for number or generate nonce for string)
}

func (ti *TableInfo) SetKeys(isAutoIncr bool, keyNames []string) *TableInfo {
	ti.KeyAutoIncr = isAutoIncr
	ti.KeyNames = keyNames
	return ti
}

func (ti *TableInfo) SetVersionCol(versionColName string) *TableInfo {
	ti.VersionColName = versionColName
	return ti
}

func (ti *TableInfo) ColNames() []string {
	names := ti.KeyAndColNames()
	ret := make([]string, 0, len(names)-len(ti.KeyNames))
nameLoop:
	for i := 0; i < len(names); i++ {
		name := names[i]
		for _, keyName := range ti.KeyNames {
			if keyName == name {
				continue nameLoop
			}
		}
		ret = append(ret, name)
	}
	return ret
}

func (ti *TableInfo) KeyAndColNames() []string {
	numFields := ti.GoType.NumField()
	ret := make([]string, 0, numFields)
	for i := 0; i < numFields; i++ {
		structField := ti.GoType.Field(i)
		sqlFieldName := strings.SplitN(structField.Tag.Get("db"), ",", 2)[0]
		if sqlFieldName == "-" {
			continue
		}
		if sqlFieldName == "" {
			sqlFieldName = FieldNameMapper(structField.Name)
		}
		ret = append(ret, sqlFieldName)
	}
	return ret
}

type Config struct {
	// *Connection // FIXME: really not sure about this now, might
	// make way more sense to lose the connection here and just
	// let the caller combine config and dbr.Session into one of
	// our sessions - possibly could make more sense for shard setup too..??

	// TableConfigMap map[reflect.Type]*TableConfig

	TableInfoMap
	//map[reflect.Type]*TableInfo
}

// NewConfig returns a new Config, properly intialized.
func NewConfig() *Config {
	return &Config{
		TableInfoMap: make(TableInfoMap),
	}
}

func (c *Config) NewConnector(defaultDbrConn *dbr.Connection, sharder Sharder) *Connector {
	return &Connector{
		Connection: &Connection{
			Connection:   defaultDbrConn,
			TableInfoMap: c.TableInfoMap,
		},
		Sharder: sharder,
	}
}

type TableInfoMap map[reflect.Type]*TableInfo

func (m TableInfoMap) AddTable(i interface{}) *TableInfo {
	return m.AddTableWithName(i, TableNameMapper(DerefType(reflect.TypeOf(i)).Name()))
}

func (m TableInfoMap) AddTableWithName(i interface{}, tableName string) *TableInfo {

	t := DerefType(reflect.TypeOf(i))
	tableInfo := m[t]
	if tableInfo != nil {
		if tableInfo.TableName != tableName {
			panic(fmt.Errorf("attempt to call AddTableWithName with a different name (expected %q, got %q)", tableInfo.TableName, tableName))
		}
		return tableInfo
	}

	tableInfo = &TableInfo{
		GoType:    t,
		TableName: tableName,
	}

	m[t] = tableInfo

	return tableInfo

}

func (m TableInfoMap) TableFor(i interface{}) *TableInfo {
	return m.TableForType(reflect.TypeOf(i))
}

func (m TableInfoMap) TableForType(t reflect.Type) *TableInfo {
	return m[DerefType(t)]
}

// type TableConfig struct {
// 	TableName string
// 	PKFields  []reflect.StructField
// 	// Hm, these could easily be handled by embedding Timestamps which have
// 	// callback methods on them (BeforeUpdate() or whatever), thus removing it
// 	// from the configuration
// 	CreateTimeField reflect.StructField
// 	UpdateTimeField reflect.StructField
// }

// func (cfg *Config) SetTypeTableName(typ reflect.Type, tableName string) {
// }

// func (cfg *Config) SetTypePkFieldNames(typ reflect.Type, pkFieldNames ...string) {
// }

// type Connection struct {
// 	*dbr.Connection
// }

// JUST IMPLEMENT LIKE THIS - IT'S FAIRLY CLOSE, AND WE'LL LEARN MORE BY USING THAN BY CONTEMPLATING

type Sharder interface {
	// can select connection and create session, and also
	// copy TableInfoMap and make mods as needed for table names
	Shard(i interface{}, keys ...interface{}) *Connection
	ShardForType(t reflect.Type, keys ...interface{}) *Connection
}

type Connector struct {

	// embed default Connection, for ease of transitioning existing code
	*Connection

	// Sharder provides a means to obtain a sharded connection.
	Sharder
}

// comes for free from *Connection
// func (c *Connector) NewSession(log dbr.EventReceiver) *Session {
// 	return c.Connection.NewSession(log)
// }

func (c *Connector) NewShardSession(log dbr.EventReceiver, i interface{}, keys ...interface{}) *Session {
	conn := c.Sharder.Shard(i, keys...)
	return conn.NewSession(log)
}

func (c *Connector) NewShardSessionForType(log dbr.EventReceiver, t reflect.Type, keys ...interface{}) *Session {
	conn := c.Sharder.ShardForType(t, keys...)
	return conn.NewSession(log)
}

type Connection struct {
	TableInfoMap

	*dbr.Connection
}

func (c *Connection) NewSession(log dbr.EventReceiver) *Session {
	sess := c.Connection.NewSession(log)
	return &Session{
		TableInfoMap: c.TableInfoMap,
		Session:      sess,
	}
}

type Session struct {
	TableInfoMap

	*dbr.Session
}

func (s *Session) Begin() (*Tx, error) {
	tx, err := s.Session.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		TableInfoMap: s.TableInfoMap,
		Tx:           tx,
	}, nil
}

func (s *Session) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := s.Session.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{
		TableInfoMap: s.TableInfoMap,
		Tx:           tx,
	}, nil
}

type Tx struct {
	TableInfoMap

	*dbr.Tx
}

// TODO:

// - figure out how connection/session relate and how this affects sharding
// - also transactions, need the same interface there
// - callbacks before and after (have a look at how GORM does this)
// - manual way to specify pks, create time, update time, anything else (table name!)
// - struct scan to get pks, create time, update time, and anything else ('dbrobj:"pk"')
// - make sure autoincrement works (even though we discourage)
// - what about optimistic locking?  (version column)
// - see where this stuff is supposed to be initialized in our overall model setup, like
//   where does AddTable() get called in an ideal scenario?  in a way that is pluggable
//   and integrates correctly with migrations...

// - for sharding, it may work to have a duplicate structure but the NewSession call
//   requires the shard key but otherwise things are basically the same...  The table
//   name should be able to be customized also though, for more complex sharding logic.

// think about related concerns and if they make sense to go in this package or elsewhere
// - upsert (need good use cases - it might make sense to accept a list of columns to
//   update when not inserting - but we really need to see what cases, e.g. the "counter
//   that starts as zero and otherwise increments is one" and another is "just make
//   sure this record exists, i don't care if it did before - lookup tables")
// - record paging (offset/limit or ideally >last_key - should support both methods)
// - record locking with timestamps and old unlock logic

func (s *Session) ObjGet(obj interface{}, pk ...interface{}) error {
	ti := s.TableInfoMap.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	fields := ti.KeyAndColNames()
	sb := s.Select(fields...)
	return objGet(sb, ti, obj, pk...)
}

func (s *Session) ObjUpdate(obj interface{}) error {
	ti := s.TableInfoMap.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	ub := s.Update(ti.TableName)
	return objUpdate(ub, ti, obj)
}

func (s *Session) ObjUpdateDiff(newObj interface{}, oldObj interface{}) error {
	ti := s.TableInfoMap.TableFor(newObj)
	if ti == nil {
		return ErrNoTable
	}
	ub := s.Update(ti.TableName)
	return objUpdateDiff(ub, ti, newObj, oldObj)

}

func (s *Session) ObjInsert(obj interface{}) error {
	ti := s.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	ib := s.InsertInto(ti.TableName)
	return objInsert(ib, ti, obj)
}

// TODO: Omitting upsert functionality for now.  It's complicated and I don't have a
// really clear set of use cases to know the behavior is what we really want.

// idea: what if we construct an INSERT ON DUPLICATE KEY UPDATE (db-specific logic) statement
// (actually, check out MERGE and see how wide the support is) and the sqlSet
// is in the form of "a = ?, b = ?" and that gets put into the update part of
// the statement.  If sqlSet is empty then it becomes a full replace.  This handles
// both the "counter" case and the "i just want to make sure this is there" case.
func (s *Session) ObjUpsert(obj interface{}, sqlSet string, args ...interface{}) error {
	return fmt.Errorf("not implemented")
}

// func (s *Session) ObjUpsert(obj interface{}) error {
// 	return fmt.Errorf("not implemented")
// }

// func (s *Session) ObjUpsertDiff(newObj interface{}, oldObj interface{}) error {
// 	return fmt.Errorf("not implemented")
// }

func (s *Session) ObjDelete(obj interface{}, pk ...interface{}) error {
	ti := s.TableInfoMap.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	db := s.DeleteFrom(ti.TableName)
	return objDelete(db, ti, obj, pk...)
}

func (tx *Tx) ObjGet(obj interface{}, pk ...interface{}) error {
	ti := tx.TableInfoMap.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	fields := ti.KeyAndColNames()
	sb := tx.Select(fields...)
	return objGet(sb, ti, obj, pk...)
}

func (tx *Tx) ObjUpdate(obj interface{}) error {
	ti := tx.TableInfoMap.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	ub := tx.Update(ti.TableName)
	return objUpdate(ub, ti, obj)
}

func (tx *Tx) ObjUpdateDiff(newObj interface{}, oldObj interface{}) error {
	ti := tx.TableInfoMap.TableFor(newObj)
	if ti == nil {
		return ErrNoTable
	}
	ub := tx.Update(ti.TableName)
	return objUpdateDiff(ub, ti, newObj, oldObj)
}

func (tx *Tx) ObjInsert(obj interface{}) error {
	ti := tx.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	ib := tx.InsertInto(ti.TableName)
	return objInsert(ib, ti, obj)
}

// func (tx *Tx) ObjUpsert(obj interface{}) error {
// 	return fmt.Errorf("not implemented")
// }

// func (tx *Tx) ObjUpsertDiff(newObj interface{}, oldObj interface{}) error {
// 	return fmt.Errorf("not implemented")
// }

func (tx *Tx) ObjDelete(obj interface{}, pk ...interface{}) error {
	ti := tx.TableInfoMap.TableFor(obj)
	if ti == nil {
		return ErrNoTable
	}
	db := tx.DeleteFrom(ti.TableName)
	return objDelete(db, ti, obj, pk...)
}

// type ShardConfig struct {
// 	TableConfigMap map[reflect.Type]*TableConfig
// 	ShardConnect   func(obj interface{}, shardKeys ...interface{}) (*Connection, error)

// 	// can be nil to indicate "use the default table name"?
// 	ShardTableName func(obj interface{}, shardKeys ...interface{}) (string, error)
// }

func pkFromObj(ti *TableInfo, obj interface{}) ([]interface{}, error) {
	var ret []interface{}
	// v := DerefValue(reflect.ValueOf(obj))
	for _, kname := range ti.KeyNames {
		fv, err := FieldValue(obj, kname)
		if err != nil {
			return nil, err
		}
		ret = append(ret, fv)
	}
	return ret, nil
}

func objGet(sb *dbr.SelectBuilder, ti *TableInfo, obj interface{}, pk ...interface{}) error {

	var err error

	// populate pk if empty
	if len(pk) == 0 {
		pk, err = pkFromObj(ti, obj)
		if err != nil {
			return err
		}
	} else if len(pk) != len(ti.KeyNames) {
		return fmt.Errorf("incorrect number of pk values, expected %d got %d", len(ti.KeyNames), len(pk))
	}

	selB := sb.From(ti.TableName)
	for j := 0; j < len(ti.KeyNames); j++ {
		selB = selB.Where(ti.KeyNames[j]+"=?", pk[j])
	}

	return selB.LoadOne(obj)
}

func objUpdate(ub *dbr.UpdateBuilder, ti *TableInfo, obj interface{}) error {

	for _, name := range ti.ColNames() {
		fv, err := FieldValue(obj, name)
		if err != nil {
			return fmt.Errorf("error reading value field %q: %v", name, err)
		}
		ub = ub.Set(name, fv)
	}

	for _, kn := range ti.KeyNames {
		fv, err := FieldValue(obj, kn)
		if err != nil {
			return fmt.Errorf("error reading key field %q: %v", kn, err)
		}
		ub = ub.Where(kn+"=?", fv)
	}

	_, err := ub.Exec()

	return err

}

func objUpdateDiff(ub *dbr.UpdateBuilder, ti *TableInfo, newObj interface{}, oldObj interface{}) error {

	for _, name := range ti.ColNames() {
		newfv, err := FieldValue(newObj, name)
		if err != nil {
			return fmt.Errorf("error reading new value field %q: %v", name, err)
		}
		oldfv, err := FieldValue(oldObj, name)
		if err != nil {
			return fmt.Errorf("error reading old value field %q: %v", name, err)
		}

		if !reflect.DeepEqual(newfv, oldfv) {
			ub = ub.Set(name, newfv)
		}

	}

	for _, kn := range ti.KeyNames {
		fv, err := FieldValue(newObj, kn)
		if err != nil {
			return fmt.Errorf("error reading key field %q: %v", kn, err)
		}
		ub = ub.Where(kn+"=?", fv)
	}

	_, err := ub.Exec()

	return err

}

func objInsert(ib *dbr.InsertBuilder, ti *TableInfo, obj interface{}) error {

	var names []string
	if ti.KeyAutoIncr {
		names = ti.ColNames()
	} else {
		names = ti.KeyAndColNames()
	}
	ib = ib.Columns(names...)
	var values []interface{}
	for _, name := range names {
		fv, err := FieldValue(obj, name)
		if err != nil {
			return fmt.Errorf("error reading field %q: %v", name, err)
		}
		values = append(values, fv)
	}
	ib = ib.Values(values...)
	res, err := ib.Exec()
	if err != nil {
		return err
	}
	if ti.KeyAutoIncr {
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		fi, err := FieldIndex(obj, ti.KeyNames[0])
		if err != nil {
			return fmt.Errorf("error finding key field %q: %v", ti.KeyNames[0], err)
		}
		v := DerefValue(reflect.ValueOf(obj))
		v.Field(fi).SetInt(id)
	}
	return err
}

func objDelete(db *dbr.DeleteBuilder, ti *TableInfo, obj interface{}, pk ...interface{}) error {

	var err error

	// populate pk if empty
	if len(pk) == 0 {
		pk, err = pkFromObj(ti, obj)
		if err != nil {
			return err
		}
	} else if len(pk) != len(ti.KeyNames) {
		return fmt.Errorf("incorrect number of pk values, expected %d got %d", len(ti.KeyNames), len(pk))
	}

	for idx, keyName := range ti.KeyNames {
		db = db.Where(keyName+"=?", pk[idx])
	}
	res, err := db.Exec()
	if err != nil {
		return err
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if ra < 1 {
		return dbr.ErrNotFound
	}
	return nil
}

func DerefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func DerefValue(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

var ErrNoField = fmt.Errorf("field not found")
var ErrNoTable = fmt.Errorf("table not found for object/type")

type fieldIndexCacheKey struct {
	Type         reflect.Type
	SQLFieldName string
}

var fieldIndexCache = make(map[fieldIndexCacheKey]int, 16)
var fieldIndexMutex sync.Mutex

func FieldIndex(obj interface{}, sqlFieldName string) (out int, rete error) {

	t := DerefType(reflect.TypeOf(obj))

	fieldIndexMutex.Lock()
	ret, ok := fieldIndexCache[fieldIndexCacheKey{t, sqlFieldName}]
	fieldIndexMutex.Unlock()
	if ok {
		return ret, nil
	}

	// record result in cache if not error
	defer func() {
		if rete == nil {
			fieldIndexMutex.Lock()
			fieldIndexCache[fieldIndexCacheKey{t, sqlFieldName}] = out
			fieldIndexMutex.Unlock()
		}
	}()

	for j := 0; j < t.NumField(); j++ {
		f := t.Field(j)
		dbName := strings.SplitN(f.Tag.Get("db"), ",", 2)[0]
		// explicitly skip "-" db tags
		if dbName == "-" {
			return -1, ErrNoField
		}
		if dbName == sqlFieldName {
			return j, nil
		}
		dbName = CamelToSnake(f.Name)
		if dbName == sqlFieldName {
			return j, nil
		}
	}

	return -1, ErrNoField

}

func FieldValue(obj interface{}, sqlFieldName string) (interface{}, error) {

	i, err := FieldIndex(obj, sqlFieldName)
	if err != nil {
		return nil, err
	}

	v := DerefValue(reflect.ValueOf(obj))
	return v.Field(i).Interface(), nil

}
