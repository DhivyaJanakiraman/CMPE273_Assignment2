package main

import (
	"encoding/json"
	"fmt"
	"github.com/drone/routes"
	"github.com/mkilling/goejdb"
	"github.com/naoina/toml"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
)

/*
type Profile struct {
	Email          string `json:"email"`
	Zip            string `json:"zip"`
	Country        string `json:"country"`
	Profession     string `json:"profession"`
	Favorite_color string `json:"favorite_color"`
	Is_smoking     string `json:"is_smoking"`
	Favorite_sport string `json:"favorite_sport"`
	Food           struct {
		Type          string `json:"type"`
		Drink_alcohol string `json:"drink_alcohol"`
	} `json:"food"`
	Music struct {
		SpotifyUserID string `json:"spotify_user_id"`
	} `json:"music"`
	Movie struct {
		Movies  []string `json:"movies"`
		TvShows []string `json:"tv_shows"`
	} `json:"movie"`
	Travel struct {
		Flight struct {
			Seat string `json:"seat"`
		} `json:"flight"`
	} `json:"travel"`
}
*/
type tomlConfig struct {
	Replication struct {
		RpcServerPortNum int
		Replica          []string
	}
	Database struct {
		FileName string
		PortNum  int
	}
}

var config tomlConfig
var oid_mappings = make(map[string]string)

type Listener int

func (l *Listener) GetLine(bsrec []byte, ack *bool) error {

	var jb, jb_err = goejdb.Open(config.Database.FileName, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("userprofile", nil)

	if jb_err != nil {
		os.Exit(1)
	}

	var currProfile map[string]interface{} //create a map and convert the json to string to store it in the map
	bson.Unmarshal(bsrec, &currProfile)
	email := currProfile["email"].(string)

	//Check if a record already exists
	query := fmt.Sprintf(`{"email" : "%s"}`, email)
	res, _ := coll.Find(query) // Name starts with 'Bru' string
	fmt.Printf("\n\nRecords found: %d\n", len(res))

	if len(res) == 0 {
		//POST
		res, _ := coll.SaveBson(bsrec)
		fmt.Printf("\nSaved Record")
		oid_mappings[email] = res
	} else {

		//PUT
		oid := oid_mappings[email]
		coll.RmBson(oid)
		delete(oid_mappings, email)

		res, _ := coll.SaveBson(bsrec)
		fmt.Printf("\nSaved Record")
		oid_mappings[email] = res

	}

	//coll.SaveBson(bsrec)
	//fmt.Printf("\nSaved Record")
	jb.Close()

	return nil
}

func GetUserProfile(w http.ResponseWriter, req *http.Request) {
	var jb, _ = goejdb.Open(config.Database.FileName, goejdb.JBOREADER)
	var coll, _ = jb.CreateColl("userprofile", nil)
	vars := req.URL.Query()     //Get all the request parameters from the URL
	email := vars.Get(":email") //Get the emailid from the URL

	// execute the query...
	query := fmt.Sprintf(`{"email" : "%s"}`, email)
	res, _ := coll.Find(query) // Name starts with 'Bru' string
	fmt.Printf("\n\nRecords found: %d\n", len(res))

	var m map[string]interface{}

	if len(res) > 0 {
		bson.Unmarshal(res[0], &m)
		mapBson, _ := json.Marshal(m) //Get the profile object corresponding to email key and marshal it to JSON
		w.Write([]byte(mapBson))      //Write the JSON to response
	} else {
		w.Write([]byte("Email does not exist."))
	}
	jb.Close()
}

func PostUserProfile(rw http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body) //Read the http body
	if err != nil {
		log.Println(err.Error())
	}

	var profile map[string]interface{}
	err = json.Unmarshal(body, &profile) //Unmarshall the json into a map
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(profile)

	var jb, jb_err = goejdb.Open(config.Database.FileName, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("userprofile", nil)

	if jb_err != nil {
		os.Exit(1)
	}

	// Now execute queryzz
	query := fmt.Sprintf(`{"email" : "%s"}`, profile["email"])
	res, _ := coll.Find(query) // Name starts with 'Bru' string
	fmt.Printf("\n\nRecords found: %d\n", len(res))
	email := profile["email"].(string)

	if len(res) == 0 {
		// Insert one record:
		bsrec, _ := bson.Marshal(profile)
		oid, _ := coll.SaveBson(bsrec)
		fmt.Printf("\nSaved Record")
		rw.WriteHeader(http.StatusCreated)

		oid_mappings[email] = oid

		// Split on comma.
		result := strings.Split(config.Replication.Replica[0], "//")

		// Display all elements.
		for i := range result {
			fmt.Println(result[i])
		}

		value := config.Replication.Replica[0]

		// Take substring from index 4 to length of string.
		substring := value[7:len(value)]
		fmt.Println(substring)

		client, err := rpc.Dial("tcp", substring)
		if err != nil {
			log.Fatal(err)
		}

		var reply bool

		err = client.Call("Listener.GetLine", bsrec, &reply)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("Record Already Exist.")
		rw.Write([]byte("Record Already Exist."))
	}
	jb.Close()

}

func PutUserProfile(rw http.ResponseWriter, req *http.Request) {

	var jb, _ = goejdb.Open(config.Database.FileName, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("userprofile", nil)

	vars := req.URL.Query()     //Get all the request parameters from the URL
	email := vars.Get(":email") //Get the emailid from the URL

	query := fmt.Sprintf(`{"email" : "%s"}`, email) //check the match for email
	res, _ := coll.Find(query)                      // Match Email
	fmt.Printf("\n\nRecords found: %d\n", len(res))

	if len(res) > 0 { //if a record match is found
		var currProfile map[string]interface{} //create a map and convert the json to string to store it in the map
		bson.Unmarshal(res[0], &currProfile)

		body, err := ioutil.ReadAll(req.Body) //Read the http body (json format)
		log.Println(string(body))

		if err != nil {
			log.Println(err.Error())
		}

		var newProfile map[string]interface{}
		err = json.Unmarshal(body, &newProfile) //Unmarshall the json into a map
		if err != nil {
			log.Println(err.Error())
		}
		log.Println(newProfile)

		for key, value := range newProfile { //for each key, value in the new profile (from put body), update the corresponding key in already existing profile

			currProfile[key] = value
		}

		//delete the oldBson
		oldBson, _ := oid_mappings[email]
		coll.RmBson(oldBson)
		delete(oid_mappings, email)

		bsrec, _ := bson.Marshal(currProfile)
		res, _ := coll.SaveBson(bsrec)
		fmt.Printf("\nSaved Record")
		rw.WriteHeader(http.StatusNoContent)

		oid_mappings[email] = res

		value := config.Replication.Replica[0]

		// Take substring from index 4 to length of string.
		substring := value[7:len(value)]
		fmt.Println(substring)

		client, err := rpc.Dial("tcp", substring)
		if err != nil {
			log.Fatal(err)
		}

		var reply bool

		err = client.Call("Listener.GetLine", bsrec, &reply)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		rw.Write([]byte("Email does not exist."))
	}
	jb.Close()

}

func DeleteUserProfile(rw http.ResponseWriter, req *http.Request) {

	var jb, _ = goejdb.Open(config.Database.FileName, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("userprofile", nil)

	vars := req.URL.Query()
	email := vars.Get(":email")

	query := fmt.Sprintf(`{"email" : "%s"}`, email)
	res, _ := coll.Find(query)
	fmt.Printf("\n\nRecords found: %d\n", len(res))

	if len(res) > 0 {

		oid := oid_mappings[email]
		coll.RmBson(oid)
		delete(oid_mappings, email)

		fmt.Printf("\nDeleted Record")
		rw.WriteHeader(http.StatusNoContent)
		/*
			value := config.Replication.Replica[0]

			// Take substring from index 4 to length of string.
			substring := value[7:len(value)]
			fmt.Println(substring)

			client, err := rpc.Dial("tcp", substring)
			if err != nil {
				log.Fatal(err)
			}

			var reply bool

			err = client.Call("Listener.GetLine", bsrec, &reply)
			if err != nil {
				log.Fatal(err)
			}
		*/
	} else {
		rw.Write([]byte("Email does not exist."))
	}
	jb.Close()

}

func main() {

	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	if err := toml.Unmarshal(buf, &config); err != nil {
		panic(err)
	}

	//Create and close the database
	var jb, _ = goejdb.Open(config.Database.FileName, goejdb.JBOCREAT)
	jb.Close()

	//server
	var my_port = fmt.Sprintf(":%d", config.Database.PortNum)

	mux := routes.New()
	mux.Get("/profile/:email", GetUserProfile)
	mux.Post("/profile", PostUserProfile)
	mux.Put("/profile/:email", PutUserProfile)
	mux.Del("/profile/:email", DeleteUserProfile)

	http.Handle("/", mux)
	log.Println("Listening...")

	go func() {
		http.ListenAndServe(my_port, nil)
	}()

	addy, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", config.Replication.RpcServerPortNum))
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}

	listener := new(Listener)
	rpc.Register(listener)
	rpc.Accept(inbound)

}
