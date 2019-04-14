package main
import (
    //"errors"
    "fmt"
//"flag"
"encoding/json"
   // "sync"
    //"time"
//rejson "github.com/secondspass/go-rejson"
//"github.com/nitishm/go-rejson"
    "github.com/go-redis/redis"
//redisgo "github.com/gomodule/redigo/redis"
)

var redisdb *redis.Client
type jsonvalue struct {
    Page   int      `json:"page"`
    Fruits []string `json:"fruits"`
}

type fileDescriptor struct{
        digest string
        serverIp string
        requestedServerIps string
        filePath string

}

//type MyStruct struct{}

func (m *jsonvalue) MarshalBinary() ([]byte, error) {
    return json.Marshal(m)
}

func(m *jsonvalue)UnmarshalBinary(data []byte) error{
  // convert data to yours, let's assume its json data
  return json.Unmarshal(data, m)
}

func main(){
	redisdb := redis.NewClusterClient(&redis.ClusterOptions{
        	Addrs: []string{"192.168.0.213:7000", "192.168.0.213:7001", "192.168.0.213:7002", "192.168.0.213:7003", "192.168.0.213:7004", "192.168.0.213:7005"},//[]string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
    	})
    	redisdb.Ping()

	err := redisdb.Set("key", "value", 0).Err()
    if err != nil {
        panic(err)
    }

    val, err := redisdb.Get("key").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println("key", val)
	//rh := rejson.NewReJSONHandler()
    //flag.Parse()
	//rh.SetGoRedisClient(redisdb)
	data := &jsonvalue{Page: 1, Fruits: []string{"apple1", "peach1"}}
	err = redisdb.Set("key2", data, 0).Err()
        if err != nil {
                panic(err)
        }
	value, err := redisdb.Get("key2").Result()
	var newdata jsonvalue
	if err := newdata.UnmarshalBinary([]byte(value)); err !=nil{
		panic(err)
	}

if _, err := redisdb.HMSet("sha:11111", map[string]interface{}{
        "digest": "sha256:123333333",
        "serverIp": "192.168.0.213", //NANNAN SET TO the first registry ip address for global dedup; even for local dedup, it is correct?!
        "requestedServerIps": "", // NANNAN: set to none initially.
        "filePath": "/home/nannan/distribution"}).Result(); err != nil {
        panic(err)
    }
reply, err := redisdb.Do("HMGET", "sha:11111", "digest", "filePath", "serverIp").Result()
fmt.Printf("%v\n", reply)
var desc fileDescriptor
//err = redisdb.HVals("sha:11111").ScanSlice(&desc)
//fmt.Printf("%d", desc)
//json.Unmarshal(redisdb.HVals("sha:11111"), &desc)
//fmt.Printf("%d", desc)
/*
var desc fileDescriptor
    if _, err = redisgo.Scan(reply, &desc.digest, &desc.filePath, &desc.serverIp); err != nil {
        
        panic( err)
    }
	fmt.Printf("des %v\n", desc)*/
	fmt.Printf("value: %v\n", newdata)

}
