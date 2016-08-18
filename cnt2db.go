package main

import ("log"
	"fmt"
	"flag"
	"bufio"
	"os"
	"regexp"
	"github.com/Bowery/prompt"
	"github.com/boltdb/bolt"
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
	comre := regexp.MustCompile(`#.*$`) // comments

	// create the transaction
	terr := db.Update(func(tx *bolt.Tx) error {

		// parse the file by line
		scanner := bufio.NewScanner(cfd)
		block := ""
		var bucket *bolt.Bucket // empty placeholder to start, holds current bucket
		for scanner.Scan() {
			line := scanner.Text()

			// strip comments
			nocLine := comre.ReplaceAllString(line, "")

			// look for block starts
			if bsm := blre.FindStringSubmatch(nocLine) ; bsm != nil { // start of a new block
				block = bsm[1] // 0 is the whole match
				var berr error
				bucket, berr = tx.CreateBucketIfNotExists([]byte(block))
				if berr != nil {
					return fmt.Errorf("Error creating/opening bucet for block %s: %v", block, err)
				}
			} else if csm := clre.FindStringSubmatch(nocLine) ; csm != nil { // device count for current block
				device := csm[1]
				cnt := csm[2]
				perr := bucket.Put([]byte(device), []byte(cnt))
				if perr != nil {
					return fmt.Errorf("Put error for key %s, val %s: %v", device, cnt, perr)
				}
			}
		}
		if serr := scanner.Err(); serr != nil {
			return fmt.Errorf("Error reading file %s: %v", countFile, serr)
		}
		return nil // commit the transaction
	})
	if terr != nil {
		return fmt.Errorf("Error creating read/write db transaction for db file %s and count file %s: %v", 
			newDB, countFile, err)
	}

	defer db.Close()

	return nil // normal exit, no error
}

// open an interactive command line to query the provided db
func dbInteract(dbName string) error {

	// open the database
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		return fmt.Errorf("dbInteract error opening supplied database file %s: %v", dbName, err)
	}

	// create regexp to parse inputs
	lre := regexp.MustCompile(`^\s*(\w+)\s*$`) // single works only - blockname or quit

	// create a read-only transaction
	terr := db.View(func(tx *bolt.Tx) error {

		// command prompt loop - exit on q or quit or exit
		for { // only exit via return
			resp, perr := prompt.Basic("db> ", true)
			if perr != nil {
				// catch ctl-c and ctl-d for exit
				eString := perr.Error()
				if (eString == "Interrupted (CTRL+C)") || (eString == "EOF (CTRL+D)") {
					return nil // normal exit
				}
				return fmt.Errorf("dbInteract error prompting for db %s: %v", dbName, perr)
			}
			if lsm := lre.FindStringSubmatch(resp) ; lsm != nil { // single word - comand or exit
				word := lsm[1]
				if (word == "quit") || (word == "q") || (word == "exit") {
					return nil
				}

				// it must be a blockname, which must match a bucket name.  look up the counts.
				b := tx.Bucket([]byte(word))
				if b == nil {
					fmt.Printf("Error, %s does not exist in the database\n", word)
				} else {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						fmt.Printf("%s: %s\n", k, v)
					}
				} 
			}
		}
	})
	if terr != nil {
		return fmt.Errorf("Error creating read-only transaction in database %s: %v", dbName, terr)
	}

	return nil // normal exit, no error
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
		if *cFile != "" {
			err := parse2db(*cFile, *odb)
			if err != nil {
				log.Fatalf("Error creating database file %s from count file %s: %v\n", *odb, *cFile, err)
			}
		} else {
			fmt.Println("Usage: cnt2db -i <database> or cnt2db -cf <countFile> -wdb <new database>")
		}
	}
}
