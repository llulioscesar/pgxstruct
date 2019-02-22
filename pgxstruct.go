package pgxstruct

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var NameMaper func(string) string = strings.ToLower

var finfos map[reflect.Type]fieldInfo
var finfoLock sync.RWMutex

var TagName = "sql"

type fieldInfo map[string][]int

func init() {
	finfos = make(map[reflect.Type]fieldInfo)
}

func getFieldInfo(typ reflect.Type) fieldInfo {
	finfoLock.RLock()
	finfo, ok := finfos[typ]
	finfoLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(fieldInfo)

	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get(TagName)

		if f.PkgPath != "" || tag == "-" {
			continue
		}

		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			for k, v := range getFieldInfo(f.Type) {
				finfo[k] = append([]int{i}, v...)
			}
			continue
		}

		if tag == "" {
			tag = f.Name
		}
		tag = NameMaper(tag)
		finfo[tag] = []int{i}
	}
	finfoLock.Lock()
	finfos[typ] = finfo
	finfoLock.Unlock()
	return finfo
}

func Scan(dest interface{}, rows *pgx.Rows) error {
	return doScan(dest, rows, "")
}

func ScanRow(dest interface{}, row *pgx.Row) error {
	return doScanRow(dest, row)
}

func ScanAliased(dest interface{}, rows *pgx.Rows, alias string) error {
	return doScan(dest, rows, alias)
}

func Columns(s interface{}) string {
	return strings.Join(cols(s), ", ")
}

func ColumnsAliased(s interface{}, alias string) string {
	names := cols(s)
	aliased := make([]string, 0, len(names))
	for _, n := range names {
		aliased = append(aliased, alias+"."+n+" AS "+alias+"_"+n)
	}
	return strings.Join(aliased, ", ")
}

func cols(s interface{}) []string {
	v := reflect.ValueOf(s)
	fields := getFieldInfo(v.Type())

	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}

	sort.Strings(names)
	return names
}

func doScan(dest interface{}, rows *pgx.Rows, alias string) error {
	destv := reflect.ValueOf(dest)
	typ := destv.Type()

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("debe ser puntero a struct; tiene %T", destv))
	}
	fieldInfo := getFieldInfo(typ.Elem())

	elem := destv.Elem()
	var values []interface{}

	cols := rows.FieldDescriptions()

	log.Println(cols)

	for _, name := range cols {
		if len(alias) > 0 {
			name.Name = strings.Replace(name.Name, alias+"_", "", 1)
		}
		idx, ok := fieldInfo[strings.ToLower(name.Name)]
		var v interface{}
		if !ok {
			v = &sql.RawBytes{}
		} else {
			v = elem.FieldByIndex(idx).Addr().Interface()
		}
		values = append(values, v)
	}
	return rows.Scan(values...)
}

func doScanRow(dest interface{}, row *pgx.Row) error {
	destv := reflect.ValueOf(dest)
	typ := destv.Type()

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("debe ser puntero a struct; tiene %T", destv))
	}
	fieldInfo := getFieldInfo(typ.Elem())

	elem := destv.Elem()
	var values []interface{}

	cols := (*pgx.Rows)(row).FieldDescriptions()

	for _, name := range cols {
		idx, ok := fieldInfo[strings.ToLower(name.Name)]
		var v interface{}
		if !ok {
			v = &sql.RawBytes{}
		} else {
			v = elem.FieldByIndex(idx).Addr().Interface()
		}
		values = append(values, v)
	}
	return row.Scan(values...)
}

func ToSnakeCase(src string) string {
	thisUpper := false
	prevUpper := false

	buf := bytes.NewBufferString("")
	for i, v := range src {
		if v >= 'A' && v <= 'Z' {
			thisUpper = true
		} else {
			thisUpper = false
		}
		if i > 0 && thisUpper && !prevUpper {
			buf.WriteRune('_')
		}
		prevUpper = thisUpper
		buf.WriteRune(v)
	}
	return strings.ToLower(buf.String())
}
