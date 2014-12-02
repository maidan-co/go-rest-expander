package expander

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

// TODO:
// 1. add filters to expansiontask in order to correctly resolve children
// 2. fix other TODOs
const (
	REF_KEY        = "ref"
	REL_KEY        = "rel"
	VERB_KEY       = "verb"
	COLLECTION_KEY = "Collection"
)

const (
	emptyTimeValue = "0001-01-01T00:00:00Z"
)

type Configuration struct {
	UsingCache           bool
	UsingMongo           bool
	MakeBulkRequest      bool
	IdURIs               map[string]string
	CacheExpInSeconds    int64
	ConnectionTimeoutInS int
}

var ExpanderConfig Configuration = Configuration{
	UsingMongo:           false,
	UsingCache:           false,
	CacheExpInSeconds:    86400, // = 24 hours
	ConnectionTimeoutInS: 2,
}

var Cache *lru.Cache = lru.New(250)
var CacheMutex = sync.Mutex{}
var client http.Client
var timeout = time.Duration(2 * time.Second)
var httpClientIsInitialized = false
var initializingHttpClient = false
var initializerMutex = sync.Mutex{}

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, timeout)
}

func Init() {
	client = http.Client{}

	client.Timeout = time.Duration(ExpanderConfig.ConnectionTimeoutInS)*time.Second

	httpClientIsInitialized = true
}

type CacheEntry struct {
	Timestamp int64
	Data      string
}

type DBRef struct {
	Collection string
	Id         interface{}
	Database   string
}

type MongoObject struct {
	Id string `json:"_id"`
}

type BulkResponseMongoObject struct {
	Data []MongoObject `json:"data"`
}

type BulkResponseData struct {
	Data []map[string]interface{} `json:"data"`
}

type ObjectId interface {
	Hex() string
}

type Filter struct {
	Children Filters
	Value    string
}

type Filters []Filter

func (m Filters) Contains(v string) bool {
	for _, m := range m {
		if v == m.Value {
			return true
		}
	}

	return false
}

func (m Filters) IsEmpty() bool {
	return len(m) == 0
}

func (m Filters) Get(v string) Filter {
	var result Filter

	if m.IsEmpty() {
		return result
	}

	for _, m := range m {
		if v == m.Value {
			return m
		}
	}

	return result
}

type ExpansionTask struct {
	Id            string
	Collection    string
	OriginalDBRef DBRef
	Success       func(value map[string]interface{})
	Error         func()
}

func (this *ExpansionTask) ResourceKey() string {
	return UniqueKey(this.Collection, this.Id)
}

type WalkStateHolder struct {
	resolveTasks *[]ExpansionTask
}

func (this *WalkStateHolder) GetExpansionTasks() []ExpansionTask {
	return *this.resolveTasks
}

func (this *WalkStateHolder) AddExpansionTask(resolveTask ExpansionTask) {
	realArray := *this.resolveTasks
	result := append(realArray, resolveTask)
	*this.resolveTasks = result
}

func UniqueKey(collection string, id string) string {
	return collection + "." + id
}

func resolveFilters(expansion, fields string) (expansionFilter Filters, fieldFilter Filters, recursiveExpansion bool, err error) {
	if !validateFilterFormat(expansion) {
		err = errors.New("expansionFilter for filtering was not correct")
		return
	}
	if !validateFilterFormat(fields) {
		err = errors.New("fieldFilter for filtering was not correct")
		return
	}

	fieldFilter, _ = buildFilterTree(fields)

	if expansion != "*" {
		expansionFilter, _ = buildFilterTree(expansion)
	} else if fields != "*" && fields != "" {
		expansionFilter, _ = buildFilterTree(fields)
	} else {
		recursiveExpansion = true
	}
	return
}

//TODO: TagFields & BSONFields
func Expand(data interface{}, expansion, fields string, headers map[string] string) map[string]interface{} {
	if ExpanderConfig.UsingMongo && len(ExpanderConfig.IdURIs) == 0 {
		fmt.Println("Warning: Cannot use mongo flag without proper IdURIs given!")
	}
	if ExpanderConfig.UsingCache && ExpanderConfig.CacheExpInSeconds == 0 {
		fmt.Println("Warning: Cannot use Cache with expiration 0, cache will be useless!")
	}

	expansionFilter, fieldFilter, recursiveExpansion, err := resolveFilters(expansion, fields)
	if err != nil {
		expansionFilter = Filters{}
		fieldFilter = Filters{}
		fmt.Printf("Warning: Filter was not correct, expansionFilter: '%v' fieldFilter: '%v', error: %v \n", expansion, fields, err)
	}

	resolveTasks := []ExpansionTask{}
	walkStateHolder := WalkStateHolder{&resolveTasks}
	expanded := walkByExpansion(data, walkStateHolder, expansionFilter, recursiveExpansion)
	executeExpansionTasks(walkStateHolder, recursiveExpansion, headers)

	filtered := walkByFilter(expanded, fieldFilter)

	return filtered
}

func executeExpansionTasks(walkStateHolder WalkStateHolder, recursive bool, headers map[string]string) {
	if ExpanderConfig.MakeBulkRequest {
		executeExpansionTasksInBulks(walkStateHolder.GetExpansionTasks(), headers)
	} else {
		executeExpansionTasksOneByOne(walkStateHolder.GetExpansionTasks(), recursive, headers)
	}
}

func executeExpansionTasksOneByOne(expansionTasks []ExpansionTask, recursive bool, headers map[string]string) {
	for _, task := range expansionTasks {
		link := buildReferenceURI(reflect.ValueOf(task.OriginalDBRef))
		value, ok := getResourceFrom(link, Filters{}, recursive, headers)
		if !ok && task.Error != nil {
			task.Error()
		} else if task.Success != nil {
			task.Success(value)
		}
	}
}

func executeExpansionTasksInBulks(expansionTasks []ExpansionTask, headers map[string]string) {
	perCollectionIds := make(map[string]string)
	for _, task := range expansionTasks {
		perCollectionIds[task.Collection] += task.Id+","
	}

	callResults := make(map[string]interface{})
	for collection, idList := range perCollectionIds {

		callURL := ExpanderConfig.IdURIs[collection] + idList
		url, _ := url.ParseRequestURI(callURL)
		value := getContentFrom(url, headers)
		ok := true
		if ok {
			bytes := []byte(value)

			var response BulkResponseMongoObject
			var responseData BulkResponseData

			_ = json.Unmarshal(bytes, &response)
			_ = json.Unmarshal(bytes, &responseData)

			for index, mongoObject := range response.Data {
				callResults[mongoObject.Id] = responseData.Data[index]
			}

		} else {
			fmt.Println("Could not resolve URL ", callURL)

		}
	}

	for _, expansionTask := range expansionTasks {
		value, ok := callResults[expansionTask.Id]

		if ok && expansionTask.Success != nil {
			expansionTask.Success(value.(map[string]interface{}))
		} else if expansionTask.Error != nil {
			expansionTask.Error()
		}
	}

}

func ExpandArray(data interface{}, expansion, fields string, headers map[string]string) []interface{} {
	if ExpanderConfig.UsingMongo && len(ExpanderConfig.IdURIs) == 0 {
		fmt.Println("Warning: Cannot use mongo flag without proper IdURIs given!")
	}
	if ExpanderConfig.UsingCache && ExpanderConfig.CacheExpInSeconds == 0 {
		fmt.Println("Warning: Cannot use Cache with expiration 0, cache will be useless!")
	}

	expansionFilter, fieldFilter, recursiveExpansion, err := resolveFilters(expansion, fields)
	if err != nil {
		expansionFilter = Filters{}
		fieldFilter = Filters{}
		fmt.Printf("Warning: Filter was not correct, expansionFilter: '%v' fieldFilter: '%v', error: %v \n", expansionFilter, fieldFilter, err)
	}

	var result []interface{}

	if data == nil {
		return result
	}

	v := reflect.ValueOf(data)
	switch data.(type) {
	case reflect.Value:
		v = data.(reflect.Value)
	}

	if v.Kind() != reflect.Slice {
		return result
	}

	v = v.Slice(0, v.Len())
	for i := 0; i < v.Len(); i++ {
		resolveTasks := []ExpansionTask{}
		walkStateHolder := WalkStateHolder{&resolveTasks}
		arrayItem := walkByExpansion(v.Index(i), walkStateHolder, expansionFilter, recursiveExpansion)
		executeExpansionTasks(walkStateHolder, recursiveExpansion, headers)
		arrayItem = walkByFilter(arrayItem, fieldFilter)
		result = append(result, arrayItem)
	}
	return result
}

func walkByFilter(data map[string]interface{}, filters Filters) map[string]interface{} {
	result := make(map[string]interface{})

	if data == nil {
		return result
	}

	for k, v := range data {
		if filters.IsEmpty() || filters.Contains(k) {
			ft := reflect.ValueOf(v)

			result[k] = v
			subFilters := filters.Get(k).Children

			if v == nil {
				continue
			}

			switch ft.Type().Kind() {
			case reflect.Map:
				result[k] = walkByFilter(v.(map[string]interface{}), subFilters)
			case reflect.Slice:
				if ft.Len() == 0 {
					continue
				}

				switch ft.Index(0).Kind() {
				case reflect.Map:
					children := make([]map[string]interface{}, 0)
				for _, child := range v.([]map[string]interface{}) {
					item := walkByFilter(child, subFilters)
					children = append(children, item)
				}
					result[k] = children
				default:
					children := make([]interface{}, 0)
				for _, child := range v.([]interface{}) {
					cft := reflect.TypeOf(child)

					if cft.Kind() == reflect.Map {
						item := walkByFilter(child.(map[string]interface{}), subFilters)
						children = append(children, item)
					} else {
						children = append(children, child)
					}
				}
					result[k] = children
				}
			}
		}
	}

	return result
}

func walkByExpansion(data interface{}, walkStateHolder WalkStateHolder, filters Filters, recursive bool) map[string]interface{} {
	result := make(map[string]interface{})

	if data == nil {
		return result
	}

	v := reflect.ValueOf(data)
	switch data.(type) {
	case reflect.Value:
		v = data.(reflect.Value)
	}
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	//	var resultWriteMutex = sync.Mutex{}
	var writeToResult = func(key string, value interface{}, omitempty bool) {
		if omitempty && isEmptyValue(reflect.ValueOf(value)) {
			delete(result, key)
		} else {
			result[key] = value
		}
	}

	// check if root is db ref
	if dbref, ok := asMongoDBRef(v); ok && recursive {
		placeholder := make(map[string]interface{})

		var resolveTask ExpansionTask

		resolveTask.OriginalDBRef = dbref
		resolveTask.Id = dbref.Id.(ObjectId).Hex()
		resolveTask.Collection = dbref.Collection
		resolveTask.Success = func(value map[string]interface{}) {
			for k, v := range value {
				placeholder[k] = v
			}
		}
		walkStateHolder.AddExpansionTask(resolveTask)
		return placeholder
	}

	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		ft := v.Type().Field(i)

		if f.Kind() == reflect.Ptr {
			f = f.Elem()
		}
		var omitempty = false

		key := ft.Name
		tag := ft.Tag.Get("json")
		if tag != "" {
			tags := strings.Split(tag, ",")
			key = tags[0]
			for _, currentPart := range tags {
				if currentPart == "omitempty" {
					omitempty = true
				}
			}
		}

		options := func() (bool, string) {
			return recursive, key
		}

		if dbref, ok := asMongoDBRef(f); ok {
			if filters.Contains(key) || recursive {

				var resolveTask ExpansionTask
				resolveTask.OriginalDBRef = dbref
				resolveTask.Id = dbref.Id.(ObjectId).Hex()
				resolveTask.Collection = dbref.Collection
				resolveTask.Success = func(value map[string]interface{}) {
					writeToResult(key, value, omitempty)
				}
				resolveTask.Error = func() {
					writeToResult(key, f.Interface(), omitempty)
				}
				walkStateHolder.AddExpansionTask(resolveTask)

			} else {
				writeToResult(key, f.Interface(), omitempty)
			}
		} else {
			val := getValue(f, walkStateHolder, filters, options)
			writeToResult(key, val, omitempty)
			switch val.(type) {
			case string:
				unquoted, err := strconv.Unquote(val.(string))
				if err == nil {
					writeToResult(key, unquoted, omitempty)
				}
			}

			if isReference(f) {
				if filters.Contains(key) || recursive {
					uri := getReferenceURI(f)
					resource, ok := getResourceFrom(uri, filters.Get(key).Children, recursive, make(map[string]string))
					if ok {
						writeToResult(key, resource, omitempty)
					}
				}
			}
		}

	}

	return result
}

func getValue(t reflect.Value, walkStateHolder WalkStateHolder, filters Filters, options func() (bool, string)) interface{} {
	recursive, parentKey := options()

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return t.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return t.Uint()
	case reflect.Float32, reflect.Float64:
		return t.Float()
	case reflect.Bool:
		return t.Bool()
	case reflect.String:
		return t.String()
	case reflect.Slice:
		var result = []interface{}{}

		for i := 0; i < t.Len(); i++ {
			current := t.Index(i)

			if filters.Contains(parentKey) || recursive {
				if isReference(current) {
					uri := getReferenceURI(current)

					//TODO: this fails in case the resource cannot be resolved, because current is DBRef not map[string]interface{}
					result = append(result, current.Interface())
					resource, ok := getResourceFrom(uri, filters.Get(parentKey).Children, recursive, make(map[string]string))
					if ok {
						result[i] = resource
					}
				} else if dbref, ok := asMongoDBRef(current); ok {
					//leave as placeholder
					result = append(result, current.Interface())

					var resolveTask ExpansionTask
					var localCounter = i
					resolveTask.OriginalDBRef = dbref
					resolveTask.Id = dbref.Id.(ObjectId).Hex()
					resolveTask.Collection = dbref.Collection
					resolveTask.Success = func(resolvedValue map[string]interface{}) {
						result[localCounter] = resolvedValue
					}
					walkStateHolder.AddExpansionTask(resolveTask)

				} else {
					result = append(result, getValue(current, walkStateHolder, filters.Get(parentKey).Children, options))
				}
			} else {
				result = append(result, getValue(current, walkStateHolder, filters.Get(parentKey).Children, options))
			}
		}

		return result
	case reflect.Map:
		result := make(map[string]interface{})

	for _, v := range t.MapKeys() {
		key := v.Interface().(string)
		result[key] = getValue(t.MapIndex(v), walkStateHolder, filters.Get(key).Children, options)
	}

		return result
	case reflect.Struct:
		val, ok := t.Interface().(json.Marshaler)
		if ok {
			bytes, err := val.(json.Marshaler).MarshalJSON()
			if err != nil {
				fmt.Println(err)
			}

			return string(bytes)
		}

		return walkByExpansion(t, walkStateHolder, filters, recursive)
	default:
		return t.Interface()
	}

	return ""
}

func getResourceFrom(u string, filters Filters, recursive bool, headers map[string]string) (map[string]interface{}, bool) {
	ok := false
	uri, err := url.ParseRequestURI(u)
	var m map[string]interface{}

	if err == nil {
		content := getContentFrom(uri, headers)
		err := json.Unmarshal([]byte(content), &m)
		if err != nil {
			return m, false
		}
		ok = true
		if hasReference(m) {
			return expandChildren(m, filters, recursive, headers), ok
		}
	}

	return m, ok
}

func expandChildren(m map[string]interface{}, filters Filters, recursive bool, headers map[string]string) map[string]interface{} {
	result := make(map[string]interface{})

	for key, v := range m {
		ft := reflect.TypeOf(v)
		result[key] = v
		if v == nil {
			continue
		}
		if ft.Kind() == reflect.Map && (recursive || filters.Contains(key)) {
			child := v.(map[string]interface{})
			uri, found := child[REF_KEY]

			if found {
				resource, ok := getResourceFrom(uri.(string), filters, recursive, headers)
				if ok {
					result[key] = resource
				}
			}
		}
	}

	return result
}

func buildReferenceURI(t reflect.Value) string {
	var uri string

	if t.Kind() == reflect.Struct {
		collection := ""
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			ft := t.Type().Field(i)

			if ft.Name == COLLECTION_KEY {
				collection = f.String()
			} else {
				objectId, ok := f.Interface().(ObjectId)
				if ok {
					base := ExpanderConfig.IdURIs[collection]
					uri = base+"/"+objectId.Hex()
				}
			}
		}
	}

	return uri
}

func asMongoDBRef(t reflect.Value) (DBRef, bool) {
	mongoEnabled := ExpanderConfig.UsingMongo && len(ExpanderConfig.IdURIs) > 0
	var dbref DBRef

	if !mongoEnabled {
		return dbref, false
	}

	if t.Kind() != reflect.Struct {
		return dbref, false

	}

	if t.NumField() != 3 {
		return dbref, false
	}

	idFound := false
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ft := t.Type().Field(i)

		if ft.Name == COLLECTION_KEY {
			dbref.Collection = f.String()
		} else if f.CanInterface() {
			objectId, ok := f.Interface().(ObjectId)
			if ok {
				dbref.Id = objectId
				idFound = true
			}
		}
	}
	ok := dbref.Collection != "" && idFound
	return dbref, ok
}

func isRefKey(ft reflect.StructField) bool {
	tag := strings.Split(ft.Tag.Get("json"), ",")[0]
	return ft.Name == REF_KEY || tag == REF_KEY
}

func isReference(t reflect.Value) bool {
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			ft := t.Type().Field(i)

			if isRefKey(ft) && t.NumField() > 1 { // at least relation & ref should be given
				return true
			}
		}
	}

	return false
}

func hasReference(m map[string]interface{}) bool {
	for _, v := range m {
		ft := reflect.TypeOf(v)

		if ft != nil && ft.Kind() == reflect.Map {
			child := v.(map[string]interface{})
			_, ok := child[REF_KEY]
			ok = ok && len(child) > 1

			if ok {
				return true
			}

			return hasReference(child)
		}
	}

	return false
}

func getReferenceURI(t reflect.Value) string {
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			ft := t.Type().Field(i)

			if isRefKey(ft) {
				return t.Field(i).String()
			}
		}
	}

	return ""
}

var makeGetCall = func(uri *url.URL, headers map[string]string) string {
	if !httpClientIsInitialized {
		initializerMutex.Lock()
		if !initializingHttpClient {
			initializingHttpClient = true
			Init()
		}
		initializerMutex.Unlock()
	}

	req, err := http.NewRequest("GET", uri.String(), nil)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)

	if err != nil {
		fmt.Println("Error while reading content of response body. It was: ", err)
	}

	return string(contents)
}

var makeGetCallAndAddToCache = func(uri *url.URL, headers map[string]string) string {
	valueToReturn := makeGetCall(uri, headers)

	var responseMap map[string]interface{}
	err := json.Unmarshal([]byte(valueToReturn), &responseMap)

	_, ok := responseMap["error"]
	if err != nil || ok {
		return ""
	}

	cacheEntry := CacheEntry{
		Timestamp: time.Now().Unix(),
		Data:      valueToReturn,
	}
	CacheMutex.Lock()
	Cache.Add(uri.String(), cacheEntry)
	CacheMutex.Unlock()
	return valueToReturn
}

var getContentFrom = func(uri *url.URL, headers map[string]string) string {
	if ExpanderConfig.UsingCache {
		CacheMutex.Lock()
		value, ok := Cache.Get(uri.String())
		CacheMutex.Unlock()
		if !ok {
			//no data found in cache
			return makeGetCallAndAddToCache(uri, headers)
		}

		cachedData := value.(CacheEntry)
		nowInMillis := time.Now().Unix()

		if nowInMillis-cachedData.Timestamp > ExpanderConfig.CacheExpInSeconds {
			//data older then Expiration
			CacheMutex.Lock()
			Cache.Remove(uri.String())
			CacheMutex.Unlock()
			return makeGetCallAndAddToCache(uri, headers)
		}

		return cachedData.Data
	}

	return makeGetCall(uri, headers)
}

func validateFilterFormat(filter string) bool {
	runes := []rune(filter)

	var bracketCounter = 0

	for i := range runes {
		if runes[i] == '(' {
			bracketCounter++
		} else if runes[i] == ')' {
			bracketCounter--
			if bracketCounter < 0 {
				return false
			}
		}
	}
	return bracketCounter == 0

}

func buildFilterTree(statement string) ([]Filter, int) {
	var result []Filter
	const comma uint8 = ','
	const openBracket uint8 = '('
	const closeBracket uint8 = ')'

	if statement == "*" {
		return result, -1
	}

	statement = strings.Replace(statement, " ", "", -1)
	if len(statement) == 0 {
		return result, -1
	}

	indexAfterSeparation := 0
	closeIndex := 0

	for i := 0; i < len(statement); i++ {
		switch statement[i] {
		case openBracket:
			filter := Filter{Value: string(statement[indexAfterSeparation:i])}
			filter.Children, closeIndex = buildFilterTree(statement[i+1:])
			result = append(result, filter)
			i = i+closeIndex
			indexAfterSeparation = i+1
			closeIndex = indexAfterSeparation
		case comma:
			filter := Filter{Value: string(statement[indexAfterSeparation:i])}
			if filter.Value != "" {
				result = append(result, filter)
			}
			indexAfterSeparation = i+1
		case closeBracket:
			filter := Filter{Value: string(statement[indexAfterSeparation:i])}
			if filter.Value != "" {
				result = append(result, filter)
			}

			return result, i+1
		}
	}

	if indexAfterSeparation > closeIndex {
		result = append(result, Filter{Value: string(statement[indexAfterSeparation:])})
	}

	if indexAfterSeparation == 0 {
		result = append(result, Filter{Value: statement})
	}

	return result, -1
}

// this function is a modification from isEmptyValue in json/encode.go
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice:
		return v.Len() == 0
	case reflect.String:
		return v.Len() == 0 || v.Interface() == emptyTimeValue
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
