package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

var (
	host   string
	port   string
	method string
	objId  string
	objVal string
)

const (
	OBJ_CREATE = "/obj/create"
	OBJ_DELETE = "/obj/delete"
	OBJ_UPDATE = "/obj/update"
	OBJ_GET    = "/obj/get"
)

func init() {

	flag.StringVar(&host, "host", "", "ip addr of raft server")
	flag.StringVar(&port, "port", "", "port of http service of raft server")
	flag.StringVar(&method, "method", "", "http method")
	flag.StringVar(&objId, "obj_id", "", "object id")
	flag.StringVar(&objVal, "obj_val", "", "object value")

	flag.Usage = usage
}

type Params struct {
	obj_id  string
	obj_val string
}

func main() {
	flag.Parse()
	var (
		err   error
		p     int
		param *Params
	)
	if p, err = strconv.Atoi(port); err != nil {
		fmt.Println("Error: invalid port, ", port)
		usage()
		return
	}
	addr := fmt.Sprintf("%s:%d", host, p)
	param = &Params{
		obj_id:  objId,
		obj_val: objVal,
	}
	fmt.Printf("params [%v]\n", param)
	switch method {
	case "get":
		val, err := getObject(addr, param)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(val)
	case "create":
		err := createObject(addr, param)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Success")
	case "delete":
		err := deleteObject(addr, param)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Success")
	case "update":
		err := updateObject(addr, param)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Success")
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `raftclt version: raftclt/1.0.0
Usage: raft [-host host] [-port port] [-obj_id objectID] [-obj_val objectValue]

Options:
`)
	flag.PrintDefaults()
}

func httpDo(url, method string, p *Params) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	q := req.URL.Query()
	q.Add("obj_id", p.obj_id)
	q.Add("obj_val", p.obj_val)
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		fmt.Errorf("http request error: %v\n", err)
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Errorf("http request error: %v\n", err)
		return nil, err
	}

	return body, nil
}

func paramsToValues(p *Params) (values url.Values) {
	fmt.Printf("params: [%v]\n", p)
	values = url.Values{}
	values.Set("obj_id", p.obj_id)
	values.Set("obj_val", p.obj_val)
	return
}

func getObject(addr string, params *Params) (objVal string, err error) {
	url := fmt.Sprintf("http://%s%s", addr, OBJ_GET)
	var res []byte
	res, err = httpDo(url, "GET", params)
	if err != nil {
		return
	}

	return string(res), nil
}

func createObject(addr string, params *Params) (err error) {
	url := fmt.Sprintf("http://%s%s", addr, OBJ_CREATE)
	var res []byte
	res, err = httpDo(url, "POST", params)
	if err != nil {
		return
	}

	return nil
}

func updateObject(addr string, params *Params) (err error) {
	url := fmt.Sprintf("http://%s%s", addr, OBJ_UPDATE)
	var res []byte
	res, err = httpDo(url, "PUT", params)
	if err != nil {
		return
	}

	return nil
}

func deleteObject(addr string, params *Params) (err error) {
	url := fmt.Sprintf("http://%s%s", addr, OBJ_DELETE)
	var res []byte
	res, err = httpDo(url, "POST", params)
	if err != nil {
		return
	}

	return nil
}