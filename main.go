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
}

func basicQueryHandler(w http.ResponseWriter, r *http.Request) {
	ch := make(chan int)
	go doBasicQuery(1, ch)
	<-ch
	fmt.Fprintf(w, "hello, mysql")
}

func handleRequests(port int) {
	http.HandleFunc("/hello_world", helloWorldHandler)
	http.HandleFunc("/basic_query", basicQueryHandler)
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
	numProcs := *numProcsPtr
	returnedProcs := runtime.GOMAXPROCS(0)
	if numProcs == 0 {
		numProcs = returnedProcs
	}
	fmt.Print("starting server on port ", *portPtr, " with ", numProcs, " threads...\n")
	handleRequests(*portPtr)
}
