package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

func migrate(source_url, dest_url string) {
	doc_batch := 10

	search_query := []byte(`{ "query": { "match_all": {} } }`)
	from := 0

	for from != -1 {
		request, err := http.NewRequest("XGET", source_url+"_search/?size="+strconv.Itoa(doc_batch)+"&from="+strconv.Itoa(from)+"&pretty=true", bytes.NewBuffer(search_query))
		request.Header.Add("Content-Type", "application/json")
		if err != nil {
			panic(err)
		}
		client := &http.Client{}
		response, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		defer response.Body.Close()
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}

		json_body := string(body)
		amount_of_docs := copyOver(json_body, dest_url)

		if amount_of_docs == doc_batch {
			from += amount_of_docs
			fmt.Println("Processed: ", from)
		} else {
			from = -1
		}
	}
}

func copyOver(json_data, dest_url string) int {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(json_data), &data)
	if err != nil {
		panic(err)
	}

	hits, ok := data["hits"].(map[string]interface{})
	if !ok {
		panic("Hits is not a map!")
	}

	docs, ok := hits["hits"].([]interface{})
	if !ok {
		panic("Hits[\"hits\"] is not an array!")
	}
	for i := 0; i < len(docs); i++ {
		doc, ok := docs[i].(map[string]interface{})
		if !ok {
			panic("Docs[i] is not a map!")
		}

		json_doc := convertMapToJSON(doc["_source"])
		upload(json_doc, dest_url)
	}

	return len(docs)
}

func convertMapToJSON(doc interface{}) []byte {
	json_doc, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}

	return json_doc
}

func upload(doc []byte, dest_url string) {
	request, err := http.NewRequest("POST", dest_url+"doc", bytes.NewBuffer(doc))
	request.Header.Add("Content-Type", "application/json")
	if err != nil {
		panic(err)
	}
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	/*body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}*/

	//str_response := string(body)
	//fmt.Println("Got response: ", str_response)
}

func main() {
	es_source_ptr := flag.String("sc", "localhost", "Source ES URL")
	es_source_port_ptr := flag.String("sp", "9200", "Source ES cluster port")
	index_source_ptr := flag.String("si", "", "Source index")
	es_dest_ptr := flag.String("dc", "localhost", "Dest ES URL")
	es_dest_port_ptr := flag.String("dp", "9200", "Dest ES cluster port")
	index_dest_ptr := flag.String("di", "", "Dest index")

	flag.Parse()

	source_url := "http://" + *es_source_ptr + ":" + *es_source_port_ptr + "/" + *index_source_ptr + "/"
	dest_url := "http://" + *es_dest_ptr + ":" + *es_dest_port_ptr + "/" + *index_dest_ptr + "/"

	migrate(source_url, dest_url)
}
