package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/gocql/gocql"
)

// GoCassa main struct
type GoCassa struct {
	session  *gocql.Session
	server   *string
	port     *int
	keyspace *string
	action   string
}

// Init function for GoCassa
func (gc *GoCassa) Init(server *string, port *int, keyspace *string, action string) (err error) {
	gc.server = server
	gc.port = port
	gc.keyspace = keyspace
	gc.action = action

	cluster := gocql.NewCluster(*gc.server)
	cluster.Keyspace = *gc.keyspace
	cluster.Port = *gc.port
	gc.session, err = cluster.CreateSession()
	if err != nil {
		return err
	}
	return nil
}

// Destroy GoCassa object
func (gc *GoCassa) Destroy() {
	defer gc.session.Close()
}

// Print set GoCassa info
func (gc *GoCassa) Print() {
	fmt.Println("Server: ", gc.server)
	fmt.Println("Port: ", gc.port)
	fmt.Println("Keyspace: ", gc.keyspace)
	fmt.Println("Action: ", gc.action)
}

// GetInfoFromID - Get object info from database
func (gc *GoCassa) GetInfoFromID(id string) map[string]string {
	var key, column1, value string
	var data map[string]string
	data = make(map[string]string)
	query := `SELECT key, column1, value FROM obj_uuid_table WHERE key = ` + textAsBlob(id, false)
	iter := gc.session.Query(query).Iter()
	for iter.Scan(&key, &column1, &value) {
		if strings.HasPrefix(column1, "type") || strings.HasPrefix(column1, "fq_name") || strings.HasPrefix(column1, "parent_type") {
			// fmt.Println("\t", column1, value)
			data[column1] = value
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal("ERROR: ", err)
	}
	return data
}

// SearchInside returns all records which contains string str
func (gc *GoCassa) SearchInside(str string) {

	var key, column1, value string
	var tables []string
	tables = append(tables, `obj_uuid_table`)
	tables = append(tables, `obj_fq_name_table`)

	for _, table := range tables {
		query := `SELECT key, column1, value FROM ` + table
		iter := gc.session.Query(query).Iter()
		for iter.Scan(&key, &column1, &value) {
			if strings.Contains(column1, str) || strings.Contains(key, str) || strings.Contains(value, str) {
				fmt.Println(key, column1, value)
			}
		}
		if err := iter.Close(); err != nil {
			log.Fatal("ERROR: ", err)
		}
	}
}

func textAsBlob(src string, underReplace bool) string {
	len := len([]byte(src))
	blob := make([]byte, hex.EncodedLen(len))
	if underReplace {
		hex.Encode(blob, []byte(strings.Replace(src, "-", "_", -1)))
	} else {
		hex.Encode(blob, []byte(src))
	}
	return "0x" + string(blob)
}

func (gc *GoCassa) findIDInFQTable(objType string, uid string) bool {
	var key, column1, value string
	// query := `SELECT key, column1, value FROM obj_fq_name_table WHERE key = ` + textAsBlob(objType, true)
	query := `SELECT key, column1, value FROM obj_fq_name_table`
	// fmt.Println(objType, textAsBlob(objType, true), uid, query)
	iter := gc.session.Query(query).Iter()
	for iter.Scan(&key, &column1, &value) {
		if strings.Contains(column1, uid) {
			return true
		}
	}
	// fmt.Println("NOT IN:", objType, uid)
	return false
}

func (gc *GoCassa) checkAllBackRefs() {
	fmt.Println("Checking back reference for all objects")
	var key, column1, value string

	find := "backref"
	query := `SELECT key, column1, value FROM obj_uuid_table`
	iter := gc.session.Query(query).Iter()
	for iter.Scan(&key, &column1, &value) {
		if strings.Contains(column1, find) {
			record := strings.Split(column1, ":")
			if !gc.findIDInFQTable(record[1], record[2]) {
				fmt.Println("NOT FOUND record in fq_name_table for:", record[2], " based on: ", column1, "Source object: ", key)
				if len(gc.GetInfoFromID(record[2])) == 0 {
					fmt.Println("!!!! OBJECT NOT FOUND at all:", record[2])
				}
			}
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal("ERROR: ", err)
	}
	fmt.Println("DONE")
}

// checkBackRefsFor will find all records which contains backrefs of ID
func (gc *GoCassa) checkBackRefsFor(id string) {
	fmt.Println("Checking back reference for: ", id)
	var key, column1, value string

	find := "backref"
	query := `SELECT key, column1, value FROM obj_uuid_table`
	iter := gc.session.Query(query).Iter()
	for iter.Scan(&key, &column1, &value) {
		if strings.Contains(column1, find) && strings.Contains(column1, find) {
			record := strings.Split(column1, ":")
			if !gc.findIDInFQTable(record[1], record[2]) {
				fmt.Println("NOT FOUND record in fq_name_table for:", record[2], " based on: ", column1, "Source object: ", key)
				if len(gc.GetInfoFromID(record[2])) == 0 {
					fmt.Println("!!!! OBJECT NOT FOUND at all:", record[2])
				}
			}
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal("ERROR: ", err)
	}
	fmt.Println("DONE")
}

func main() {

	serverPtr := flag.String("server", "127.0.0.1", "Server IP address")
	portPtr := flag.Int("port", 9041, "Cassandra port")
	keyspacePtr := flag.String("keyspace", "config_db_uuid", "Cassandra KeySpace")
	// tablePtr := flag.String("table", "obj_fq_name_table", "Table")
	// findPtr := flag.String("find", "default", "String to search in records")
	// checkBR := flag.Bool("check-backrefs", false, "Check backrefs inside Contrail cassandra database")

	flag.Parse()
	args := flag.Args()

	if args[0] == "help" || len(args) < 2 {
		PrintUsage()
		return
	}

	gc := GoCassa{}
	err := gc.Init(serverPtr, portPtr, keyspacePtr, "checkbr")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	// gc.Print()

	switch cmd := args[0]; cmd {
	case "fulltext":
		gc.SearchInside(args[1])
	case "info":
		fmt.Println("INFO for id: ", args[1])
		PrintMap(gc.GetInfoFromID(args[1]))
	case "check-backref":
		gc.checkBackRefsFor(args[1])
	case "check-all-backref":
		gc.checkAllBackRefs()
	default:
		fmt.Println("\n------> Unknow command: ", args[0])
		PrintUsage()
	}

	gc.Destroy()
}

// PrintUsage of application
func PrintUsage() {
	fmt.Println("\n Usage: gocassa [--flag [--flag]] command \n Commands:")
	PrintCmd()
	fmt.Println("\n Flags:")
	flag.PrintDefaults()
}

// PrintCmd help info
func PrintCmd() {
	fmt.Println("\t help \t\t\t Print application usage")
	fmt.Println("\t info <id> \t\t Returns base information about object with <ID>, stored in DB ")
	fmt.Println("\t fulltext <string> \t Returns all records which contains <string>")
	fmt.Println("\t check-backref <id>|all \t Check back reference inconsistency for <id> or all ids")
	fmt.Println("\t check-all-backref |all \t Check back reference inconsistency for <id> or all ids")
	fmt.Println("\t clear-backref <id>|all \t Fix stale back references for <id> or all ids")
}

// PrintMap /
func PrintMap(data map[string]string) {
	yml, _ := yaml.Marshal(&data)

	fmt.Println("----")
	fmt.Println(string(yml))
}