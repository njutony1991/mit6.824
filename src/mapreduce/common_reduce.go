package mapreduce

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	kvs := make(map[string]*list.List)
	for i := 0; i < nMap; i++ {
		name := reduceName(jobName, i, reduceTaskNumber)
		fmt.Println("DoReduce: read ", name)
		file, err := os.Open(name)
		if err != nil {
			log.Fatal("DoReduce: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = list.New()
			}
			kvs[kv.Key].PushBack(kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mergename := mergeName(jobName, reduceTaskNumber)
	file, err := os.Create(mergename)
	if err != nil {
		log.Fatal("DoReduce: create", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		var values []string
		for e := kvs[k].Front(); e != nil; e = e.Next() {
			var real_val string
			var ok bool
			if real_val, ok = e.Value.(string); !ok {
				log.Fatal("DoReduce,some wrong with the value type: Not String!!!!!")
			}
			values = append(values, real_val)
		}
		res := reduceF(k, values)
		enc.Encode(KeyValue{k, res})
	}
	file.Close()
}
