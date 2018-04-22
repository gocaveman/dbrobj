package dbrobj

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocraft/dbr"
	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

func TestSqlite3(t *testing.T) {

	assert := assert.New(t)

	dsn := fmt.Sprintf(`file:testdb%v?mode=memory&cache=shared`, time.Now().UnixNano())
	conn, err := dbr.Open("sqlite3", dsn, nil)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_ai (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	first_name TEXT,
	last_name TEXT
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_sk (
	id TEXT PRIMARY KEY,
	first_name TEXT,
	last_name TEXT
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_mk (
	category TEXT,
	id TEXT,
	first_name TEXT,
	last_name TEXT,
	PRIMARY KEY(category, id)
)
`)
	assert.NoError(err)

	runTests(t, "sqlite3", dsn)

}

func TestMysql(t *testing.T) {

	if mysqlConnStr == "" {
		t.Logf("Skipping TestMysql, use '-mysql' on command line to enable this test")
		t.SkipNow()
	}

	assert := assert.New(t)

	conn, err := dbr.Open("mysql", mysqlConnStr, nil)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_ai (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	first_name VARCHAR(255),
	last_name VARCHAR(255)
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_sk (
	id VARCHAR(255) PRIMARY KEY,
	first_name VARCHAR(255),
	last_name VARCHAR(255)
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_mk (
	category VARCHAR(255),
	id VARCHAR(255),
	first_name VARCHAR(255),
	last_name VARCHAR(255),
	PRIMARY KEY(category, id)
)
`)
	assert.NoError(err)

	runTests(t, "mysql", mysqlConnStr)

}

func TestPostgres(t *testing.T) {

	if postgresConnStr == "" {
		t.Logf("Skipping TestPostgres, use '-postgres' on command line to enable this test")
		t.SkipNow()
	}

	assert := assert.New(t)

	conn, err := dbr.Open("postgres", postgresConnStr, nil)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_ai (
	id SERIAL PRIMARY KEY,
	first_name VARCHAR(255),
	last_name VARCHAR(255)
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_sk (
	id VARCHAR(255) PRIMARY KEY,
	first_name VARCHAR(255),
	last_name VARCHAR(255)
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE person_mk (
	category VARCHAR(255),
	id VARCHAR(255),
	first_name VARCHAR(255),
	last_name VARCHAR(255),
	PRIMARY KEY(category, id)
)
`)
	assert.NoError(err)

	runTests(t, "postgres", postgresConnStr)

}

// All of the testing except sharding goes in here, and each of
// the sqlite3, mysql and postgres tests just do the connection
// and table setup and call this.
func runTests(t *testing.T, driver, dsn string) {

	assert := assert.New(t)

	type PersonAI struct {
		ID        int    `db:"id" json:"id"`
		FirstName string `db:"first_name" json:"first_name"`
		LastName  string `db:"last_name" json:"last_name"`
	}

	conn, err := dbr.Open(driver, dsn, nil)
	assert.NoError(err)

	config := NewConfig()
	config.AddTableWithName(&PersonAI{}, "person_ai").SetKeys(true, []string{"id"})

	connector := config.NewConnector(conn, nil)
	sess := connector.NewSession(nil)
	personAI := PersonAI{
		FirstName: "Joe1",
		LastName:  "Example",
	}
	assert.NoError(sess.ObjInsert(&personAI))
	if personAI.ID <= 0 {
		t.Fatalf("personAI.ID is %v instead of >= 1", personAI.ID)
	}

	personAI.FirstName = "Joe1A"
	assert.NoError(sess.ObjUpdate(&personAI))

	newPerson := personAI
	newPerson.FirstName = "Joe1B"
	assert.NoError(sess.ObjUpdateDiff(&newPerson, personAI))

	assert.NoError(sess.ObjGet(&personAI, personAI.ID))
	assert.Equal("Joe1B", newPerson.FirstName)

	tmpID := personAI.ID
	personAI.ID = 0 // make sure it's not reading from the struct
	err = sess.ObjDelete(&personAI, tmpID)
	assert.NoError(err)
	err = sess.ObjDelete(&personAI, tmpID)
	assert.Error(err)

	personAI = PersonAI{
		FirstName: "Joe1C",
		LastName:  "Example",
	}
	assert.NoError(sess.ObjInsert(&personAI))
	// fetch person object without providing pks separately
	personAI = PersonAI{ID: personAI.ID}
	assert.NoError(sess.ObjGet(&personAI))
	assert.Equal("Joe1C", personAI.FirstName)

	// delete person without providing pks separately
	assert.NoError(sess.ObjDelete(&personAI))
	var c int
	assert.NoError(sess.Select("count(1)").From("person_ai").Where("id=?", personAI.ID).LoadOne(&c))
	assert.Equal(0, c, "Count should be zero after deletion")

	// make sure that providing a wrong pk length fails
	assert.Error(sess.ObjGet(&personAI, 123, 123))

	t.Logf("TODO: upsert functionality, both use cases (REPLACE INTO... and INSERT IF DUPLICATE KEY UPDATE...)")

	// test transacations
	{

		personAI = PersonAI{
			FirstName: "Joe1",
			LastName:  "Example",
		}

		tx, err := sess.Begin()
		assert.NoError(err)

		assert.NoError(tx.ObjInsert(&personAI))
		if personAI.ID <= 0 {
			t.Fatalf("personAI.ID is %v instead of >= 1", personAI.ID)
		}
		// t.Logf("inserted personAI with ID %v", personAI.ID)

		personAI.FirstName = "Joe1A"
		assert.NoError(tx.ObjUpdate(&personAI))

		newPerson = personAI
		newPerson.FirstName = "Joe1B"
		assert.NoError(tx.ObjUpdateDiff(&newPerson, personAI))

		assert.NoError(tx.ObjGet(&personAI, personAI.ID))
		assert.Equal("Joe1B", newPerson.FirstName)

		err = tx.ObjDelete(&personAI, personAI.ID)
		assert.NoError(err)
		err = tx.ObjDelete(&personAI, personAI.ID)
		assert.Error(err)

		err = tx.Commit()
		assert.NoError(err)

		// try a rollback, just for kicks
		newPerson.ID = 0
		newPerson.FirstName = "Joe2"
		newPerson.LastName = "Example"
		tx, err = sess.Begin()
		assert.NoError(err)
		err = tx.ObjInsert(&newPerson)
		assert.NoError(err)
		var loadPerson PersonAI
		err = tx.ObjGet(&loadPerson, newPerson.ID)
		assert.NoError(err)

		err = tx.Rollback()
		assert.NoError(err)
	}

	// now try some things using string keys (no autoincrement)
	{
		type PersonSK struct {
			ID        string `db:"id" json:"id"`
			FirstName string `db:"first_name" json:"first_name"`
			LastName  string `db:"last_name" json:"last_name"`
		}

		config.AddTableWithName(&PersonSK{}, "person_sk").SetKeys(false, []string{"id"})

		personSK := PersonSK{
			ID:        "0001",
			FirstName: "Joe1",
			LastName:  "Example",
		}

		// the main thing that is different here is the inserts, the rest of the functionality
		// should work exactly the same - that allows us to limit how much we need to test here

		assert.NoError(sess.ObjInsert(&personSK))
		personSK2 := PersonSK{}
		assert.NoError(sess.ObjGet(&personSK2, "0001"))
		assert.Equal("Joe1", personSK2.FirstName) // make sure it actually loaded
		assert.NoError(sess.ObjDelete(&PersonSK{}, "0001"))

	}

	// now test multiple primary keys
	{
		type PersonMK struct {
			Category  string `db:"category" json:"category"`
			ID        string `db:"id" json:"id"`
			FirstName string `db:"first_name" json:"first_name"`
			LastName  string `db:"last_name" json:"last_name"`
		}

		config.AddTableWithName(&PersonMK{}, "person_mk").SetKeys(false, []string{"category", "id"})

		personMK := PersonMK{
			Category:  "test1",
			ID:        "0001",
			FirstName: "Joe1",
			LastName:  "Example",
		}

		// test insert and delete

		assert.NoError(sess.ObjInsert(&personMK))
		personMK2 := PersonMK{}
		assert.NoError(sess.ObjGet(&personMK2, "test1", "0001"))
		assert.Equal("Joe1", personMK2.FirstName) // make sure it actually loaded
		assert.NoError(sess.ObjDelete(&PersonMK{}, "test1", "0001"))

	}

	// func TestVersion(t *testing.T) {
	// }

}

// TestShard is completely separate, since we need multiple database instances to work with.
// We use sqlite3 for this, just for practicality.
func TestShard(t *testing.T) {
}
