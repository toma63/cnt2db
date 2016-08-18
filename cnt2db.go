package main

import ("log"
	"fmt"
	"flag"
	"bufio"
	"regexp"
	"Bowery/prompt"
	"boltdb/bolt"
)

// Parse the given block count file, populate and return a new database
func parse2db(countFile string, newDB string) error {

	// Create and open the database
	db, err := bolt.Open(newDB, 0600, nil)
	if err != nil {
		return fmt.Errorf("parse2db error opening new database file %s: %v", newDB, err)
	}

	// open the count file for read
	cfd, ferr := os.Open(countFile)
	if ferr != nil {
		return fmt.Errorf("Error opening count file %s: %v", countFile, err)
	}
	defer cfd.Close()

	// create the line parsing regexps
	blre := regexp.MustCompile(`^\s*block\s*:\s*(\w+)\s*$`) // blocknames - block: <blockname>
	clre := regexp.MustCompile(`^\s*(\w+)\s*:\s*(\d+)\s*$`) // device counts - <deviceName>: <count>

	terr := db.Update(func(tx *bolt.Tx) error {

		// parse the file by line
		scanner := bufio.NewScanner(os.Stdin)
		block := ""
		var bucket = bolt.Bucket{}
		for scanner.Scan() {
			line := scanner.Text()
			bsm := blre.FindStringSubmatch(line) // look for block starts
			if bsm != nil { // start of a new block
				block := bsm[1] // 0 is the whole match
				berr, bucket := tx.CreateBucket([]byte(block))
			}
		}
		if serr := scanner.Err(); serr != nil {
			return fmt.Errorf("Error reading file %s: %v", countFile, serr)
		}

		return nil
	})
	if terr != nil {
		return fmt.Errorf("Error creating read/write db transaction for db file %s and count file %s: %v", 
			newDB, countFile, err)
	}

	Defer db.Close()
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
