package dbrobj

import (
	"fmt"
	"reflect"
)

// type Relation struct {
// 	Name        string       // logical name of relation
// 	Type        string       // has_one, has_many, etc.
// 	RelatedType reflect.Type // might need to be RelatedTypeName (hm, or wait, we have the type from the field...)
// }

type Relation interface {
	RelationName() string
	// Load(c *Connection, o interface{}) error
}

// func NewBelongsTo(name string, objType reflect.Type, objField reflect.StructField, idField reflect.StructField) *BelongsTo {
// 	return &BelongsTo{
// 		name:     name,
// 		objType:  objType,
// 		objField: objField,
// 		idField:  idField,
// 	}
// }

type BelongsTo struct {
	Name string
	// ObjType  reflect.Type
	ObjFieldGoName string
	// ObjField reflect.StructField
	// IDField  reflect.StructField
	IDFieldGoName string
	// ObjFieldName string
}

func (r *BelongsTo) RelationName() string {
	return r.Name
}

type RelationMap map[string]Relation

func (s *Session) ObjLoadRelation(o interface{}, relationName string) error {

	ti := s.TableFor(o)
	if ti == nil {
		return fmt.Errorf("failed to find table for %T", o)
	}

	rel := ti.RelationMap[relationName]
	if rel == nil {
		return fmt.Errorf("failed to find relation named %q", relationName)
	}

	switch r := rel.(type) {
	case *BelongsTo:

		vo := derefValue(reflect.ValueOf(o))
		// to := vo.Type()

		objv := vo.FieldByName(r.ObjFieldGoName)
		// objv := vo.FieldByIndex(r.ObjField.Index)

		// get the ID field value
		// idValue := vo.FieldByIndex(r.IDField.Index)
		idValue := vo.FieldByName(r.IDFieldGoName)

		// if ID is zero value, it means we should load nothing
		if isZeroOfUnderlyingType(idValue.Interface()) {
			// if ObjField is a pointer, then we also want to set the pointer to nil
			if objv.Type().Kind() == reflect.Ptr {
				objv.Set(reflect.New(objv.Type()))
			}
			return nil
		}

		// if ObjField is a ptr and it's nil, make an empty instance
		if objv.Type().Kind() == reflect.Ptr && objv.IsNil() {
			objv.Set(reflect.New(objv.Type().Elem()))
		}

		// get the actual object we're targeting
		objvv := derefValue(objv)

		idvv := derefValue(idValue)

		// load ObjField using ObjGet and value IDField
		err := s.ObjGet(objvv.Addr().Interface(), idvv.Interface())
		if err != nil {
			return fmt.Errorf("BelongsTo relation load error: %v", err)
		}
		return nil
	}

	return fmt.Errorf("unknown relation type %T", rel)
}

// TODO:

// complex query

// load single relation (optionally with where)

// helper to remove records from join where not in set?  or can they just do this with dbr... (damn, irritating zero elements special case!)

// DeleteJoinStringNotIn
// Example: DeleteJoinStringNotIn("book_category", "book_id", in.BookID, "category_id", in.CategoryIDs...)
// func (s *Session) DeleteJoinStringNotIn(tableName string, matchField string, matchValue interface{}, delFieldName string, delIds ...string) error {
// }

// func (s *Session) DeleteJoinInt64NotIn(tableName string, matchField string, matchValue interface{}, delFieldName string, delIds ...int64) error {
// }

// helper to insert where not in set?  or can they do this with the upsert call... (need a multi version)

// Example: UpsertJoinString("book_category", "book_id", in.BookID, "category_id", in.CategoryIDs...)
// func (s *Session) UpsertJoinString(tableName string, matchField string, matchValue interface{}, delFieldName string, delIds ...string) error {
// }

/*
Basically these two helpers from dbrobj would make these relations easy to implement:
```
sess.DeleteJoinStringNotIn("book_category", "book_id", bookID, "category_id", categoryIDs...)
sess.UpsertJoinString("book_category", "book_id", bookID, "category_id", categoryIDs...)
```
And then I guess to follow our pattern, you’d end up wrapping these with corresponding methods on the store that already know the type and the ID fields (i.e. this is what the calls in the controller would look like):
```
store.DeleteBookCategoriesNotIn(in.BookID, in.CategoryIDs...)
store.UpsertBookCategories(in.BookID, in.CategoriesIDs...)
```
*/

func isZeroOfUnderlyingType(x interface{}) bool {
	return reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
}
