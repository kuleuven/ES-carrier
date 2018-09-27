package main

import "fmt"
import "flag"

func migrate(source_url, dest_url string) {
	fmt.Println("Source cluster:", source_url)
	fmt.Println("Dest cluster:", dest_url)
}

func main() {
	es_source_ptr := flag.String("sc", "localhost", "Source ES URL")
	es_source_port_ptr := flag.String("sp", "9200", "Source ES cluster port")
	index_source_ptr := flag.String("si", "", "Source index")
	es_dest_ptr := flag.String("dc", "localhost", "Dest ES URL")
	es_dest_port_ptr := flag.String("dp", "9200", "Dest ES cluster port")
	index_dest_ptr := flag.String("di", "", "Dest index")

	flag.Parse()

	source_url := *es_source_ptr + ":" + *es_source_port_ptr + "/" + *index_source_ptr + "/"
	dest_url := *es_dest_ptr + ":" + *es_dest_port_ptr + "/" + *index_dest_ptr + "/"

	migrate(source_url, dest_url)
}
