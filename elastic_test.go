package air_elasticclient

import (
	"context"
	"github.com/airingone/config"
	"github.com/airingone/log"
	"github.com/olivere/elastic/v7"
	"reflect"
	"testing"
)

//es client 测试
func TestMysqlClient(t *testing.T) {
	config.InitConfig()                     //配置文件初始化
	log.InitLog(config.GetLogConfig("log")) //日志初始化
	InitEsClient("elastic_test")            //初始化es client
	defer CloseEsClient()

	index := "index04"
	//创建索引,可以写doc时自动生成(但自动生成的会比较多其他默认字段)
	err := EsCreateIndex(context.Background(), "elastic_test", index, exampleMapping)
	log.Error("EsCreateIndex: err: %+v", err)

	//写数据doc
	var data exampleData
	data.Userid = "user01"
	data.Username = "username01"
	data.Userdesc = "user01"
	data.Usertel = 137
	id, err := EsInsertDoc(context.Background(), "elastic_test", index, "1", data)
	log.Error("EsInsertDoc: id: %s, err: %+v", id, err)

	//读doc
	data1, err := EsGetDoc(context.Background(), "elastic_test", index, "1")
	log.Error("EsGetDoc: data: %s, err: %+v", data1, err)

	//search
	termQuery := elastic.NewTermQuery("userid", "user01")
	result, err := EsTermSearch(context.Background(), "elastic_test", index, termQuery)
	log.Error("EsTermSearch: total: %d, err: %+v", result.TotalHits(), err)
	if result.TotalHits() > 0 {
		for _, item := range result.Each(reflect.TypeOf(data)) {
			if u, ok := item.(exampleData); ok {
				log.Error("EsTermSearch: data: %+v", u)
			}
		}
	}

	//delete doc
	ret, err := EsDeleteDoc(context.Background(), "elastic_test", index, "1")
	log.Error("EsDeleteDoc: ret: %+v, err: %+v", ret, err)

	//or
	//write doc
	esConfig := config.GetEsConfig("elastic_test")
	cli, err := NewEsClient(esConfig.Addr, esConfig.UserName, esConfig.Password, esConfig.TimeOutMs)
	if err != nil {
		log.Error("new es client err, err: %+v", err)
		return
	}
	id2, err := cli.InsertDoc(context.Background(), index, "1", data)
	log.Error("EsInsertDoc: id2: %s, err: %+v", id2, err)
	cli.Close()

	//kinaba dev: GET index02/_mapping
	//GET index03/_doc/1
	//GET index04/_search
	//{
	//  "query": {
	//    "term": {
	//      "userid": "user01"
	//    }
	//  }
	//}
}
