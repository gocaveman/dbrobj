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
}

type RelationMap map[string]Relation

type belongsTo struct {
	name           string
	objFieldGoName string
	idFieldGoName  string
}

func NewBelongsTo(name, objFieldGoName, idFieldGoName string) Relation {
	return &belongsTo{
		name:           name,
		objFieldGoName: objFieldGoName,
		idFieldGoName:  idFieldGoName,
	}
}

func (r *belongsTo) RelationName() string {
	return r.name
}

type hasMany struct {
	name                string
	sliceFieldGoName    string
	otherIDFieldSQLName string
}

func NewHasMany(name, sliceFieldGoName, otherIDFieldSQLName string) Relation {
	return &hasMany{
		name:                name,
		sliceFieldGoName:    sliceFieldGoName,
		otherIDFieldSQLName: otherIDFieldSQLName,
	}
}

func (r *hasMany) RelationName() string {
	return r.name
}

func (s *Session) ObjLoadRelations(o interface{}, relationNames ...string) error {

	ti := s.TableFor(o)
	if ti == nil {
		return fmt.Errorf("failed to find table for %T", o)
	}

	for _, relationName := range relationNames {

		rel := ti.RelationMap[relationName]
		if rel == nil {
			return fmt.Errorf("failed to find relation named %q", relationName)
		}

		switch r := rel.(type) {

		case *belongsTo:

			vo := derefValue(reflect.ValueOf(o))

			objv := vo.FieldByName(r.objFieldGoName)

			// get the ID field value
			idValue := vo.FieldByName(r.idFieldGoName)

			// if ID is zero value, it means we should load nothing
			if isZeroOfUnderlyingType(idValue.Interface()) {
				// if objFieldGoName refers to a pointer, then we also want to set the pointer to nil
				if objv.Type().Kind() == reflect.Ptr {
					objv.Set(reflect.New(objv.Type()))
				}
				return nil
			}

			// if objFieldGoName refers to a ptr and it's nil, make an empty instance
			if objv.Type().Kind() == reflect.Ptr && objv.IsNil() {
				objv.Set(reflect.New(objv.Type().Elem()))
			}

			// get the actual object we're targeting
			objvv := derefValue(objv)

			idvv := derefValue(idValue)

			// load object field using ObjGet and value of id field
			err := s.ObjGet(objvv.Addr().Interface(), idvv.Interface())
			if err != nil {
				return fmt.Errorf("belongsTo relation load error: %v", err)
			}

		case *hasMany:

			vo := derefValue(reflect.ValueOf(o))

			objv := vo.FieldByName(r.sliceFieldGoName)
			if objv.Type().Kind() != reflect.Slice {
				return fmt.Errorf("hasMany relation field %q is not a slice", r.sliceFieldGoName)
			}
			elType := objv.Type().Elem()
			elDType := derefType(elType)

			otherTI := s.TableForType(elDType)
			if otherTI == nil {
				return fmt.Errorf("no table info for type %s", elDType.String())
			}

			if len(ti.KeyNames) != 1 {
				return fmt.Errorf("table info for %q requires exactly one key, found %d instead", ti.TableName, len(ti.KeyNames))
			}

			pkVal, err := fieldValue(vo.Interface(), ti.KeyNames[0])
			if err != nil {
				return fmt.Errorf("failed to find primary key value on type %s: %v", vo.Type().Name(), err)
			}

			_, err = s.Select(otherTI.KeyAndColNames()...).From(otherTI.TableName).
				Where(r.otherIDFieldSQLName+"=?", pkVal).Load(objv.Addr().Interface())
			if err != nil {
				return fmt.Errorf("error selecting hasMany relation %q of type %s: %v", r.RelationName(), elDType.String(), err)
			}

		default:
			return fmt.Errorf("unknown relation type %T", rel)

		}

	}

	return nil
}

func (s *Session) ObjLoadRelationWhere(o interface{}, relationName string, whereClause string, whereArgs []interface{}, limit, offset uint64) error {
	return fmt.Errorf("not implemented")
}

type FieldSpec string

type WhereSpec string // FIXME: figure out

// list of fields
// where clause
// joins

func (s *Session) ObjQuery(oList interface{}, fieldSpecs ...FieldSpec, whereClause WhereSpec, whereArgs []interface{}, limit, offset uint64) error {

	// s.Select("").From("")

	return nil
}

// TODO:

// complex query for belongs_to and has_many

// read and write relations for ids - but that's specifically for join tables

// TODO:

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
