package main

import ("log"
	"fmt"
	"flag"
	"Bowery/prompt"
	"boltdb/bolt"
)

// Parse the given block count file, populate and return a new database
func parse2db(countFile string, newDB string) error {
}

// open an interactive command line to query the provided db
func dbInteract(dbName string) error {
}

func main() {
	// define flags
	idb := flag.String("i", "", "database for interactive queries")
	odb := flag.String("wdb", "", "new database which will be populated with count file data")
	cFile := flag.String("cf", "", "count file to parse and store in a new database")

	// parse command line flags
	flag.Parse()

	// check flags
	if (*idb != "" && ((*odb != "") || (*cFile != "")))  {
		log.Fatal("-wdb and -cf may not be specified in interactive mode")
	}
	if (*cFile != "" && *odb == "") || (*cFile == "" && *odb != ""){
		log.Fatal("-wdb and -cf must be specified together")
	}

	// launch interactive query session
	if *idb != "" {
		err := dbInteract(*idb)
		if err != nil {
			log.Fatalf("Error from interactive query session for database %s: %v\n", *idb, err)
		}
	} else {
		// create new database from count file
		if *cFile != nil {
			err := parse2db(*cFile, *odb)
			if err != nil {
				log.Fatalf("Error creating database file %s from count file %s: %v\n", *odb, *cFile, err)
			}
		} else {
			fmt.Println("Usage: cnt2db -i <database> or cnt2db -cf <countFile> -wdb <new database>")
		}
}
