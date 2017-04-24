package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

// TODO: current endpoints access hardcoded (not in the query itself, but still
//       hardcoded elsewhere) DB ids instead of allowing them to be passed in
//       the URL.

var db *sql.DB

func helloWorldHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello, world")
}

func doBasicQuery(id int, ch chan int) {
	rows, err := db.Query("SELECT 1")
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var value int
	for rows.Next() {
		rows.Scan(&value)
	}
	ch <- value
	close(ch)
}

func doParentQuery(id int, ch chan int) {
	rows, err := db.Query("SELECT * FROM `buzz` WHERE `id`=?", id)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var buzzId, col1 int
	var col2, col3, col4, col5, col6, col7, col8, col9, createdAt, updatedAt string
	for rows.Next() {
		rows.Scan(&buzzId, &col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &createdAt, &updatedAt)
	}
	ch <- 1
	close(ch)
}

func doFullQuery(id int, ch chan int) {
	rows, err := db.Query("SELECT * FROM `buzz` INNER JOIN `sub_buzz` ON `buzz`.`id`=`sub_buzz`.`buzz_id` WHERE `buzz`.`id`=?", id)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var buzzId, col1, subId, subBuzzId, subCol1 int
	var col2, col3, col4, col5, col6, col7, col8, col9, createdAt, updatedAt string
	var subCol2, subCol3, subCol4, subCol5, subCol6, subCol7, subCol8, subCol9, subCreatedAt, subUpdatedAt string
	for rows.Next() {
		rows.Scan(&buzzId, &col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &createdAt, &updatedAt, &subId, &subBuzzId, &subCol1, &subCol2, &subCol3, &subCol4, &subCol5, &subCol6, &subCol7, &subCol8, &subCol9, &subCreatedAt, &subUpdatedAt)
	}
	ch <- 1
	close(ch)
}

func basicQueryHandler(w http.ResponseWriter, r *http.Request) {
	ch := make(chan int)
	go doBasicQuery(1, ch)
	<-ch
	fmt.Fprintf(w, "hello, mysql")
}

func parentQueryHandler(w http.ResponseWriter, r *http.Request) {
	ch := make(chan int)
	go doParentQuery(1, ch)
	<-ch
	fmt.Fprintf(w, "hello, mysql")
}

func fullQueryHandler(w http.ResponseWriter, r *http.Request) {
	ch := make(chan int)
	go doFullQuery(1, ch)
	<-ch
	fmt.Fprintf(w, "hello, mysql")
}

func handleRequests(port int) {
	http.HandleFunc("/hello_world", helloWorldHandler)
	http.HandleFunc("/basic_query", basicQueryHandler)
	http.HandleFunc("/parent_query", parentQueryHandler)
	http.HandleFunc("/full_query", fullQueryHandler)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), nil))
}

func main() {
	portPtr := flag.Int("port", 42002, "port to run on")
	numProcsPtr := flag.Int("gomaxprocs", 0, "GOMAXPROCS")
	flag.Parse()
	var err error
	db, err = sql.Open("mysql", "root:@tcp(localhost:3306)/mysql_benchmark?charset=utf8&autocommit=false")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	db.SetMaxIdleConns(32)
	numProcs := *numProcsPtr
	returnedProcs := runtime.GOMAXPROCS(0)
	if numProcs == 0 {
		numProcs = returnedProcs
	}
	fmt.Print("starting server on port ", *portPtr, " with ", numProcs, " threads...\n")
	handleRequests(*portPtr)
}
