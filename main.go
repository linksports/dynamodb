package dynamodb

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsDynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/dynamo"
)

// DynamodbConfig :
type DynamodbConfig struct {
	Endpoint string
	Region   string
}

// DynamodbResponse :
type DynamodbResponse struct {
}

// DynamodbPaged :
type DynamodbPaged struct {
	Limit    int
	PageKeys []*DynamodbAttributeValue
}

// ScanFilter :
type ScanFilter struct {
	Expr  string
	Value interface{}
}

// DynamodbOperator is an operation to apply in key comparisons.
type DynamodbOperator int

type DynamodbAttributeValue struct {
	Key   string
	Value interface{}
}

// Operators used for comparing against the range key in queries.
const (
	DynamodbEqual DynamodbOperator = iota
	DynamodbNotEqual
	DynamodbLess
	DynamodbLessOrEqual
	DynamodbGreater
	DynamodbGreaterOrEqual
	DynamodbBeginsWith
	DynamodbBetween
)

func (o *DynamodbOperator) value() dynamo.Operator {
	if o == nil {
		return dynamo.Equal
	}
	return [...]dynamo.Operator{dynamo.Equal, dynamo.NotEqual, dynamo.Less, dynamo.LessOrEqual, dynamo.Greater, dynamo.GreaterOrEqual, dynamo.BeginsWith, dynamo.Between}[*o]
}

// DynamodbOrder :
type DynamodbOrder bool

// Order
const (
	DynamodbOrderAsc  DynamodbOrder = true
	DynamodbOrderDesc DynamodbOrder = false
)

// DynamodbOptions :
type DynamodbOptions struct {
	Operator *DynamodbOperator
	Order    *DynamodbOrder
}

func (o *DynamodbOrder) value() dynamo.Order {
	if o == nil {
		return true
	}
	if *o {
		return true
	}
	return false
}

// LocalSecondaryIndexName :
type LocalSecondaryIndexName string

// DynamodbKey :
type DynamodbKey struct {
	Hash                func() (string, interface{})
	Range               func() (string, interface{}, *DynamodbOptions)
	LocalSecondaryIndex func() (LocalSecondaryIndexName, string, interface{}, *DynamodbOptions)
}

// Dynamodb :
type Dynamodb interface {
	Get(tableName string, key DynamodbKey, result interface{}) error
	GetAll(tableName string, key DynamodbKey, result interface{}) error
	BatchGet(tableName string, keys []*DynamodbKey, result interface{}) error
	Count(tableName string, key DynamodbKey) (int64, error)
	Paging(tableName string, key DynamodbKey, paged DynamodbPaged, result interface{}) error
	Put(tableName string, item interface{}) (*DynamodbResponse, error)
	Delete(tableName string, key DynamodbKey) (*DynamodbResponse, error)
	Scan(tableName string, result interface{}, filters ...ScanFilter) error

	ExistsTable(name string) bool
	CreateTable(name string, entity interface{}) error
	CreateTableWithLocalSecondaryIndex(name string, entity interface{}, indexName string) error
	DeleteTable(name string) error
}

type dynamodb struct {
	db *dynamo.DB
}

func New(sess *session.Session, config *DynamodbConfig) (Dynamodb, error) {
	return BuildDynamodb(sess, config)
}

// BuildDynamodb :
func BuildDynamodb(sess *session.Session, config *DynamodbConfig) (Dynamodb, error) {
	client, err := connectDynamodb(sess, config)
	if err != nil {
		return nil, err
	}
	return &dynamodb{client}, nil
}

func query(table *dynamo.Table, key DynamodbKey) *dynamo.Query {
	hKey, hValue := key.Hash()
	req := table.Get(hKey, hValue)

	if key.Range != nil {
		rKey, rValue, option := key.Range()

		op := DynamodbEqual
		if option != nil {
			if _op := option.Operator; _op != nil {
				op = *_op
			}

			if order := option.Order; order != nil {
				return req.Range(rKey, op.value(), rValue).Order(order.value())
			}
		}

		return req.Range(rKey, op.value(), rValue)
	}

	if key.LocalSecondaryIndex != nil {
		lName, lKey, lValue, option := key.LocalSecondaryIndex()

		op := DynamodbEqual
		if option != nil {
			if _op := option.Operator; _op != nil {
				op = *_op
			}

			if order := option.Order; order != nil {
				return req.Index(string(lName)).Range(lKey, op.value(), lValue).Order(order.value())
			}
		}

		return req.Index(string(lName)).Range(lKey, op.value(), lValue)
	}
	return req
}

func (con *dynamodb) Get(tableName string, key DynamodbKey, result interface{}) error {
	table := con.db.Table(tableName)
	return query(&table, key).One(result)
}

func (con *dynamodb) GetAll(tableName string, key DynamodbKey, result interface{}) error {
	table := con.db.Table(tableName)
	return query(&table, key).All(result)
}

func (con *dynamodb) BatchGet(tableName string, keys []*DynamodbKey, result interface{}) error {
	if len(keys) < 1 {
		return errors.New("key empty")
	}

	m := make(map[interface{}]bool)
	uniqKeys := []*DynamodbKey{}

	for _, key := range keys {
		_, hValue := key.Hash()

		if !m[hValue] {
			m[hValue] = true
			uniqKeys = append(uniqKeys, key)
		}
	}

	itemKeyNames := make([]string, 2)
	itemKeys := make([]dynamo.Keyed, len(uniqKeys))

	for i, k := range uniqKeys {
		hKey, hValue := k.Hash()

		if i == 0 {
			itemKeyNames[0] = hKey
		}

		if f := k.Range; f != nil {
			rKey, rValue, _ := f()
			if i == 0 {
				itemKeyNames[1] = rKey
			}
			itemKeys[i] = dynamo.Keys{hValue, rValue}
		} else {
			itemKeys[i] = dynamo.Keys{hValue}
		}
	}

	table := con.db.Table(tableName)
	if err := table.Batch(itemKeyNames...).Get(itemKeys...).All(result); err != nil {
		return err
	}

	return nil
}

func (con *dynamodb) Count(tableName string, key DynamodbKey) (int64, error) {
	table := con.db.Table(tableName)
	return query(&table, key).Count()
}

func (con *dynamodb) Paging(tableName string, key DynamodbKey, paged DynamodbPaged, result interface{}) error {
	pagingKey := map[string]*awsDynamodb.AttributeValue{}
	for _, attr := range paged.PageKeys {
		switch value := attr.Value.(type) {
		case int:
		case int8:
		case int16:
		case int32:
		case int64:
			pagingKey[attr.Key] = &awsDynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(int(value))),
			}
		case string:
			pagingKey[attr.Key] = &awsDynamodb.AttributeValue{
				S: aws.String(value),
			}
		}
	}

	table := con.db.Table(tableName)
	return query(&table, key).StartFrom(pagingKey).Limit(int64(paged.Limit)).All(result)

}

func (con *dynamodb) Put(tableName string, item interface{}) (*DynamodbResponse, error) {
	err := con.db.Table(tableName).Put(item).Run()
	return &DynamodbResponse{}, err
}

func (con *dynamodb) Delete(tableName string, key DynamodbKey) (*DynamodbResponse, error) {
	hKey, hValue := key.Hash()
	req := con.db.Table(tableName).Delete(hKey, hValue)

	var err error
	if key.Range != nil {
		rKey, rValue, _ := key.Range()
		err = req.Range(rKey, rValue).Run()
	} else {
		err = req.Run()
	}

	return &DynamodbResponse{}, err
}

// ScanFilter is only for script. Do not use from application.
// Expr is
// attribute_exists (path)
// attribute_not_exists (path)
// attribute_type (path, type)
// begins_with (path, substr)
// contains (path, operand)
// size (path)
func (con *dynamodb) Scan(tableName string, result interface{}, filters ...ScanFilter) error {
	if len(filters) > 0 {
		tmp := con.db.Table(tableName).Scan()
		for _, f := range filters {
			tmp.Filter(f.Expr, f.Value)
		}
		return tmp.All(result)
	}

	return con.db.Table(tableName).Scan().All(result)
}

func connectDynamodb(sess *session.Session, dbConfig *DynamodbConfig) (*dynamo.DB, error) {
	config := aws.NewConfig().WithRegion(dbConfig.Region)

	if len(dbConfig.Endpoint) > 0 {
		config = config.WithEndpoint(dbConfig.Endpoint)
	}

	db := dynamo.New(sess, config)
	return db, nil
}

func (con *dynamodb) ExistsTable(name string) bool {
	list, _ := con.db.ListTables().All()

	for _, tableName := range list {
		fmt.Println(tableName)
		if tableName == name {
			return true
		}
	}

	return false
}

func (con *dynamodb) CreateTable(name string, entity interface{}) error {
	return con.db.CreateTable(name, entity).Run()
}

func (con *dynamodb) CreateTableWithLocalSecondaryIndex(name string, entity interface{}, indexName string) error {
	return con.db.CreateTable(name, entity).Project(indexName, dynamo.KeysOnlyProjection).Run()
}

func (con *dynamodb) DeleteTable(name string) error {
	return con.db.Table(name).DeleteTable().Run()
}
