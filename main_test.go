package dynamodb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/bxcodec/faker/v3"
	"github.com/stretchr/testify/assert"
)

const tableNameHashOnly = "hash-only"

type HashOnly struct {
	Id        string `dynamo:"ID,hash"`
	Name      string `dynamo:"Name"`
	Status    int    `dynamo:"Status"`
	CreatedAt string `dynamo:"CreatedAt"`
}

func (HashOnly) HashKey() string {
	return "ID"
}

const tableNameHashAndRange = "hash-and-range"

type HashAndRange struct {
	Id        string `dynamo:"ID,hash"`
	CreatedAt string `dynamo:"CreatedAt,range"`
	Name      string `dynamo:"Name"`
	Status    int    `dynamo:"Status"`
}

func (HashAndRange) HashKey() string {
	return "ID"
}
func (HashAndRange) RangeKey() string {
	return "CreatedAt"
}

func newDynamo(t *testing.T) Dynamodb {
	sess := session.New()
	db, err := New(sess, &DynamodbConfig{
		Endpoint: "http://localhost:8000",
		Region:   "us-east-1",
	})

	if t == nil {
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(99)
		}
	} else {
		if ok := assert.NoError(t, err); !ok {
			fmt.Println(err.Error())
			t.FailNow()
		}
	}

	return db
}

func TestMain(m *testing.M) {
	db := newDynamo(nil)

	if !db.ExistsTable(tableNameHashOnly) {
		if err := db.CreateTable(tableNameHashOnly, HashOnly{}); err != nil {
			fmt.Println(err.Error())
			os.Exit(99)
		}
	}

	if !db.ExistsTable(tableNameHashAndRange) {
		if err := db.CreateTable(tableNameHashAndRange, HashAndRange{}); err != nil {
			fmt.Println(err.Error())
			os.Exit(99)
		}
	}

	status := m.Run()

	if err := db.DeleteTable(tableNameHashOnly); err != nil {
		fmt.Println("Delete table(hash-only) failure")
	}

	if err := db.DeleteTable(tableNameHashAndRange); err != nil {
		fmt.Println("Delete table(hash-and-range) failure")
	}

	os.Exit(status)
}

func TestPut(t *testing.T) {
	dynamo := newDynamo(t)

	t.Run("Hash only", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			var expect HashOnly
			faker.FakeData(&expect)

			_, err := dynamo.Put(tableNameHashOnly, &expect)
			assert.NoError(t, err)
		})

		t.Run("Success: duplicate hashkey", func(t *testing.T) {
			var expect HashOnly
			faker.FakeData(&expect)

			_, err := dynamo.Put(tableNameHashOnly, &expect)
			assert.NoError(t, err)

			_, err = dynamo.Put(tableNameHashOnly, &expect)
			assert.NoError(t, err)
		})

		t.Run("Failure", func(t *testing.T) {
			t.Run("hashkey blank", func(t *testing.T) {
				var expect HashOnly
				faker.FakeData(&expect)
				expect.Id = ""

				_, err := dynamo.Put(tableNameHashOnly, &expect)
				assert.Error(t, err)
			})
		})
	})

	t.Run("Hash and Range", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			var expect HashAndRange
			faker.FakeData(&expect)

			_, err := dynamo.Put(tableNameHashAndRange, &expect)
			assert.NoError(t, err)
		})

		t.Run("Success: duplicate hashkey and rangekey", func(t *testing.T) {
			var expect HashAndRange
			faker.FakeData(&expect)

			_, err := dynamo.Put(tableNameHashAndRange, &expect)
			assert.NoError(t, err)

			_, err = dynamo.Put(tableNameHashAndRange, &expect)
			assert.NoError(t, err)
		})

		t.Run("Failure", func(t *testing.T) {
			t.Run("hashkey blank", func(t *testing.T) {
				var expect HashAndRange
				faker.FakeData(&expect)
				expect.Id = ""

				_, err := dynamo.Put(tableNameHashAndRange, &expect)
				assert.Error(t, err)
			})

			t.Run("rangekey blank", func(t *testing.T) {
				var expect HashAndRange
				faker.FakeData(&expect)
				expect.CreatedAt = ""

				_, err := dynamo.Put(tableNameHashAndRange, &expect)
				assert.Error(t, err)
			})
		})
	})
}

func TestDelete(t *testing.T) {
	dynamo := newDynamo(t)
	t.Run("Hash only", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			var expect HashOnly
			faker.FakeData(&expect)
			dynamo.Put(tableNameHashOnly, &expect)

			dynamo.Delete(tableNameHashOnly, DynamodbKey{
				Hash: func() (string, interface{}) { return HashOnly{}.HashKey(), string(expect.Id) },
			})
		})

		t.Run("Success: not exists", func(t *testing.T) {
			var expect HashOnly
			faker.FakeData(&expect)
			dynamo.Put(tableNameHashOnly, &expect)

			dynamo.Delete(tableNameHashOnly, DynamodbKey{
				Hash: func() (string, interface{}) { return HashOnly{}.HashKey(), "not-exists" },
			})
		})
	})

	t.Run("Hash and Range", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			var expect HashAndRange
			faker.FakeData(&expect)
			dynamo.Put(tableNameHashOnly, &expect)

			op := DynamodbEqual
			options := &DynamodbOptions{
				Operator: &op,
			}
			dynamo.Delete(tableNameHashAndRange, DynamodbKey{
				Hash: func() (string, interface{}) { return HashAndRange{}.HashKey(), string(expect.Id) },
				Range: func() (string, interface{}, *DynamodbOptions) {
					return HashAndRange{}.HashKey(), expect.CreatedAt, options
				},
			})
		})

		t.Run("Success: not exists", func(t *testing.T) {
			var expect HashAndRange
			faker.FakeData(&expect)
			dynamo.Put(tableNameHashOnly, &expect)

			op := DynamodbEqual
			options := &DynamodbOptions{
				Operator: &op,
			}
			dynamo.Delete(tableNameHashAndRange, DynamodbKey{
				Hash: func() (string, interface{}) { return HashAndRange{}.HashKey(), "not-exists" },
				Range: func() (string, interface{}, *DynamodbOptions) {
					return HashAndRange{}.HashKey(), "not-exists", options
				},
			})
		})
	})
}

func TestGet(t *testing.T) {
	dynamo := newDynamo(t)

	t.Run("Hash only", func(t *testing.T) {
		var expect HashOnly
		faker.FakeData(&expect)
		expect.Id = faker.UUIDDigit()

		dynamo.Put(tableNameHashOnly, &expect)

		t.Run("Exists", func(t *testing.T) {
			var datum HashOnly
			err := dynamo.Get(tableNameHashOnly, DynamodbKey{
				Hash: func() (string, interface{}) { return HashOnly{}.HashKey(), string(expect.Id) },
			}, &datum)

			assert.NoError(t, err)
			assert.Equal(t, expect, datum)
		})

		t.Run("Not exists", func(t *testing.T) {
			var datum HashOnly
			err := dynamo.Get(tableNameHashOnly, DynamodbKey{
				Hash: func() (string, interface{}) { return HashOnly{}.HashKey(), string("invalid") },
			}, &datum)

			assert.Error(t, err)
		})
	})

	t.Run("Hash and Range", func(t *testing.T) {
		var expect HashAndRange
		faker.FakeData(&expect)

		dynamo.Put(tableNameHashAndRange, &expect)

		op := DynamodbEqual
		option := &DynamodbOptions{
			Operator: &op,
		}

		t.Run("Success", func(t *testing.T) {
			var datum HashAndRange

			err := dynamo.Get(tableNameHashAndRange, DynamodbKey{
				Hash: func() (string, interface{}) { return HashAndRange{}.HashKey(), string(expect.Id) },
				Range: func() (string, interface{}, *DynamodbOptions) {
					return HashAndRange{}.RangeKey(), expect.CreatedAt, option
				},
			}, &datum)

			assert.NoError(t, err)
			// assert.Equal(t, expect, datum)
		})

		t.Run("Not exists", func(t *testing.T) {
			var datum HashOnly
			err := dynamo.Get(tableNameHashAndRange, DynamodbKey{
				Hash: func() (string, interface{}) { return HashAndRange{}.HashKey(), string("invalid") },
				Range: func() (string, interface{}, *DynamodbOptions) {
					return HashAndRange{}.RangeKey(), expect.CreatedAt, option
				},
			}, &datum)

			assert.Error(t, err)
		})
	})
}

func TestCount(t *testing.T) {
	dynamo := newDynamo(t)

	hashKey := faker.UUIDDigit()
	now := time.Now()

	// includes
	n := rand.Intn(20) + 1
	for i := 0; n > i; i++ {
		var d HashAndRange
		faker.FakeData(&d)
		d.Id = hashKey
		d.CreatedAt = now.AddDate(0, 0, i).String()
		dynamo.Put(tableNameHashAndRange, &d)
	}

	// excludes
	for i := 0; rand.Intn(10)+1 > i; i++ {
		var d HashAndRange
		faker.FakeData(&d)
		d.Id = hashKey
		d.CreatedAt = now.AddDate(0, 0, -i).String()
		dynamo.Put(tableNameHashAndRange, &d)
	}

	op := DynamodbGreater
	order := DynamodbOrderAsc
	option := &DynamodbOptions{
		Operator: &op,
		Order:    &order,
	}

	count, err := dynamo.Count(tableNameHashAndRange, DynamodbKey{
		Hash: func() (string, interface{}) { return HashAndRange{}.HashKey(), hashKey },
		Range: func() (string, interface{}, *DynamodbOptions) {
			return HashAndRange{}.RangeKey(), now.AddDate(0, 0, -1).String(), option
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, n, int(count))
}

func TestPaging(t *testing.T) {
	dynamo := newDynamo(t)

	pageSize := 2
	itemsCount := pageSize*rand.Intn(10) + 1

	hashKey := faker.UUIDDigit()
	now := time.Now()

	for i := 0; itemsCount > i; i++ {
		var item HashAndRange
		faker.FakeData(&item)
		item.Id = hashKey
		item.CreatedAt = now.AddDate(0, 0, i).String()
		dynamo.Put(tableNameHashAndRange, &item)
	}

	op := DynamodbGreater
	order := DynamodbOrderAsc
	option := &DynamodbOptions{
		Operator: &op,
		Order:    &order,
	}

	pageCount := 0
	endCursor := now.AddDate(0, 0, -1).String()

	for {
		var page []*HashAndRange
		err := dynamo.Paging(
			tableNameHashAndRange,
			DynamodbKey{
				Hash: func() (string, interface{}) { return HashAndRange{}.HashKey(), hashKey },
				Range: func() (string, interface{}, *DynamodbOptions) {
					return HashAndRange{}.RangeKey(), now.AddDate(0, 0, -2).String(), option
				},
			},
			DynamodbPaged{
				Limit: pageSize,
				PageKeys: []*DynamodbAttributeValue{
					{
						Key:   HashAndRange{}.HashKey(),
						Value: hashKey,
					},
					{
						Key:   HashAndRange{}.RangeKey(),
						Value: endCursor,
					},
				},
			},
			&page,
		)
		assert.NoError(t, err)
		if len(page) < pageSize {
			break
		}
		pageCount++
		endCursor = page[len(page)-1].CreatedAt
	}

	assert.Equal(t, itemsCount/pageSize, pageCount)
}
