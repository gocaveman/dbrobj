package dbrobj

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocraft/dbr"
	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

// NOTES:
// rel_name is optional and defaults to the snake_case version of the field name (or maybe the json name),
//   but we do need it in order to refer to it in queries, etc.

// QUESTIONS:
// Should we support table names? Probably a good idea, although still need ability to prefix.

type Author struct {
	AuthorID   string `db:"author_id" dbrobj:"pk"`
	NomDePlume string `db:"nom_de_plume"`

	Books []Book `db:"-" dbrobj:"has_many"`
}

type Publisher struct {
	PublisherID string `db:"publisher_id" dbrobj:"pk"`
	CompanyName string `db:"company_name"`

	Books []Book `db:"-" dbrobj:"has_many,rel_name=books"`
}

type Book struct {
	BookID string `db:"book_id" dbrobj:"pk"`

	AuthorID string  `db:"author_id" dbrobj:""`
	Author   *Author `db:"-" dbrobj:"belongs_to,rel_key=author_id"`

	PublisherID string     `db:"publisher_id" dbrobj:""`
	Publisher   *Publisher `db:"-" dbrobj:"belongs_to"`

	Title string `db:"title"`

	Categories []Category `db:"-"`

	CategoryIDs []string `db:"-" dbrobj:"belongs_to_many_ids,through=book_category"`
}

type BookCategory struct {
	BookID     string `db:"publisher_id" dbrobj:"pk"`
	CategoryID string `db:"category_id" dbrobj:"pk"`
}

type Category struct {
	CategoryID string `db:"category_id" dbrobj:"pk"`
	Name       string `db:"name"`
	Books      []Book `db:"-" dbrobj:"belongs_to_many,through=book_category"`
}

func TestBelongsTo(t *testing.T) {

	assert := assert.New(t)

	dsn := fmt.Sprintf(`file:TestBelongsTo%v?mode=memory&cache=shared`, time.Now().UnixNano())
	conn, err := dbr.Open("sqlite3", dsn, nil)
	assert.NoError(err)

	// FIXME: add foreign key constraints for a more complete scenario

	_, err = conn.Exec(`
CREATE TABLE author (
	author_id VARCHAR(64),
	nom_de_plume TEXT,
	PRIMARY KEY(author_id)
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE book (
	book_id VARCHAR(64),
	author_id VARCHAR(64),
	publisher_id VARCHAR(64),
	title VARCHAR(255),
	PRIMARY KEY(book_id)
)
`)
	assert.NoError(err)

	_, err = conn.Exec(`
CREATE TABLE publisher (
	publisher_id VARCHAR(64),
	company_name VARCHAR(255),
	PRIMARY KEY(publisher_id)
)
`)
	assert.NoError(err)

	config := NewConfig()
	config.AddTableWithName(&Author{}, "author").SetKeys(false, []string{"author_id"}).AddRelation(NewHasMany("books", "Books", "author_id"))
	config.AddTableWithName(&Publisher{}, "publisher").SetKeys(false, []string{"publisher_id"})
	config.AddTableWithName(&Book{}, "book").SetKeys(false, []string{"book_id"}).AddRelation(NewBelongsTo("author", "Author", "AuthorID"))

	connector := config.NewConnector(conn, nil)
	sess := connector.NewSession(nil)

	assert.NoError(sess.ObjInsert(&Author{
		AuthorID:   "author_0001",
		NomDePlume: "Bill Shakespeare",
	}))
	assert.NoError(sess.ObjInsert(&Publisher{
		PublisherID: "publisher_0001",
		CompanyName: "Classic Funk Publishing",
	}))
	assert.NoError(sess.ObjInsert(&Book{
		BookID:      "book_0001",
		AuthorID:    "author_0001",
		PublisherID: "publisher_0001",
		Title:       "Macbeth",
	}))
	assert.NoError(sess.ObjInsert(&Book{
		BookID:      "book_0002",
		AuthorID:    "author_0001",
		PublisherID: "publisher_0001",
		Title:       "Hamlet",
	}))

	book := Book{}
	assert.NoError(sess.ObjGet(&book, "book_0001"))
	assert.NoError(sess.ObjLoadRelations(&book, "author"))
	assert.Equal("Bill Shakespeare", book.Author.NomDePlume)

	author := Author{}
	assert.NoError(sess.ObjGet(&author, "author_0001"))
	assert.NoError(sess.ObjLoadRelations(&author, "books"))
	assert.Len(author.Books, 2)

	t.Logf("Relation loaded: %#v", author)

}
