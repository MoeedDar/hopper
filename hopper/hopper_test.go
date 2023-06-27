package hopper

import (
	"fmt"
	"testing"
)

func TestInsert(t *testing.T) {
	values := []Map{
		{
			"name": "Foo",
			"age":  10,
		},
		{
			"name": "Bar",
			"age":  88.3,
		},
		{
			"name": "Baz",
			"age":  10,
		},
	}

	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")
	for i, data := range values {
		id, err := db.Insert("users", data)
		if err != nil {
			t.Fatal(err)
		}
		if id != uint64(i+1) {
			t.Fatalf("expect ID %d got %d", i, id)
		}

	}
	// users, err := db.Find("users", Filter{})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if len(users) != len(values) {
	// 	t.Fatalf("expecting %d result got %d", len(values), len(users))
	// }
}

func TestFind(t *testing.T) {
	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")

	values := []Map{
		{
			"name": "Dave",
			"age":  3,
		},
		{
			"name": "Davey",
			"age":  69,
		},
		{
			"name": "Bob",
			"age":  42,
		},
		{
			"name": "Alice",
			"age":  69,
		},
		{
			"name": "Carol",
			"age":  32,
		},
		{
			"name": "Dawid",
			"age":  21,
		},
		{
			"name": "David",
			"age":  12,
		},
	}

	for _, v := range values {
		if _, err := db.Insert("people", v); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.PrintCollection("people"); err != nil {
		t.Fatal(err)
	}

	f := db.Find("people", 30)
	f.Contains(Map{"name": "Da"}, false)
	result, err := f.Exec()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%v\n", result)

	f = db.Find("people", 4)
	f.Gt(Map{"age": 30}, false)
	f.Contains(Map{"name": "Da"}, true)
	result, err = f.Exec()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%v\n", result)

	// data := Map{
	// 	"name":    "Foobarbar",
	// 	"isAdmin": true,
	// }
	// id, err := db.Insert("auth", data)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if id != 1 {
	// 	t.Fatalf("expecting id 1 got %d", id)
	// }
	// results, err := db.Find("auth", Filter{})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if len(results) != 1 {
	// 	t.Fatalf("expecting 1 result got %d", len(results))
	// }
	// result := results[0]
	// if result["name"] != data["name"] {
	// 	t.Fatalf("expected %s got %s", data["name"], result["name"])
	// }
	// if result["isAdmin"] != data["isAdmin"] {
	// 	t.Fatalf("expected %b got %b", data["isAdmin"], result["isAdmin"])
	// }
}
