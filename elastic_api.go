package air_elasticclient

import (
	"context"
	"errors"
	"github.com/airingone/config"
	"github.com/airingone/log"
	"github.com/olivere/elastic/v7"
	"sync"
)

//es 操作接口封装，这里可以在使用的时候扩展常用的接口

var AllEsClients map[string]*EsClient //全局es client
var AllEsClientsRmu sync.RWMutex

//初始化全局Es对象
//configName: 配置名
func InitEsClient(configName ...string) {
	if AllEsClients == nil {
		AllEsClients = make(map[string]*EsClient)
	}

	for _, name := range configName {
		config := config.GetEsConfig(name)
		cli, err := NewEsClient(config.Addr, config.UserName, config.Password, config.TimeOutMs)
		if err != nil {
			log.Error("[ES]: InitEsClient err, config name: %s, err: %+v", name, err)
			continue
		}

		AllEsClientsRmu.Lock()
		if oldCli, ok := AllEsClients[name]; ok { //如果已存在则先关闭
			oldCli.Close()
		}
		AllEsClients[name] = cli
		AllEsClientsRmu.Unlock()
		log.Info("[ES]: InitEsClient succ, config name: %s", name)
	}
}

//close all client
func CloseEsClient() {
	if AllEsClients == nil {
		return
	}
	AllEsClientsRmu.RLock()
	defer AllEsClientsRmu.RUnlock()
	for _, cli := range AllEsClients {
		cli.Close()
	}
}

//get es client
//configName: 配置名
func GetEsClient(configName string) (*EsClient, error) {
	AllEsClientsRmu.RLock()
	defer AllEsClientsRmu.RUnlock()
	if _, ok := AllEsClients[configName]; !ok {
		return nil, errors.New("es client not exist")
	}

	return AllEsClients[configName], nil
}

//自定义mapping举例与说明
//https://github.com/olivere/elastic/blob/3db0060fd8cb964465de85d8062407472d6b8f46/setup_test.go
const exampleMapping = `
{
	"mappings":{
		"properties":{
			"userid":{
				"type":"keyword",
				"store": true,
				"fielddata": true
			},
			"username":{
				"type":"text",
				"boost": 2
			},
			"userdesc":{
				"type":"text"
			},
			"usertel":{
				"type":"long"
			}
		}
	}
}
`

type exampleData struct {
	Userid   string `json:"userid"`
	Username string `json:"username"`
	Userdesc string `json:"userdesc"`
	Usertel  uint64 `json:"usertel"`
}

/*const exampleMapping = `
{
	"mappings":{
		"properties":{
			"userid":{
				"type":"text",
				"store": true,
				"fielddata": true,
				"copy_to": "userdesc"
			},
			"username":{
				"type":"text",
				"analyzer": "ik_smart",
				"boost": 2,
				"copy_to": "userdesc"
			},
			"userdesc":{
				"type":"text"
			},
			"usertel":{
				"type":"long"
			}
		}
	}
}
`
*/
//mappings: map关键字
//properties: 参数关键字，将保存的对象数据放在这里
//store: 字段属性，这个field的数据将会被单独存储。这时候，如果你要求返回field1（store：yes），es会分辨出field1已经被存储了，因此不会从_source中加载，而是从field1的存储块中加载
//fielddata: 字段属性，开启指定字段的fielddat，默认为关闭，如果开启（当你发起一个查询，分析字符串的聚合将会被加载到 fielddata，如果这些字符串之前没有被加载过，会消耗内存）
//analyzer: 字段属性，分词器，默认standard，具体支持需要根据需要
//boost: 字段属性，查询时字段相关性得分，默认为1
//copy_to: 字段属性，该属性允许将多个字段的值copy到指定字段,如上面将userid与username拷贝到userdesc字段
//fields: 字段属性，子字段
//index: 字段属性，是否为索引，默认true，false表示不记录索引，也就不可搜索
//index_options: 字段属性，控制倒排索引记录的内容
//null_value: 字段属性，当字段遇到null值时候的处理策略（字段为null时候是不能被搜索的,也就是说，text类型的字段不能使用该属性），设置该值后可以用你设置的值替换null值，这点可类比mysql中的"default"设置默认值
//search_analyzer: 字段属性，指定搜索时分词器，默认standard
//userid:写入数据userid字段，类型类text，进行存储，

//EsCreateIndex创建索引
//ctx: context
//configName: es配置项名称
//dataIndex: index
//mapping: 数据内容格式
func EsCreateIndex(ctx context.Context, configName string, indexName string, mapping string) error {
	cli, err := GetEsClient(configName)
	if err != nil {
		return err
	}

	return cli.CreateIndex(ctx, indexName, mapping)
}

//EsInsertDoc写数据项（写入为json）,指定id创建数据
//ctx: context
//configName: es配置项名称
//dataIndex: 数据index
//dataId: 数据id
//data: 数据内容，写入到Es为json格式，data数据可以是struct json或string json
func EsInsertDoc(ctx context.Context, configName string, dataIndex string, dataId string, data interface{}) (string, error) {
	cli, err := GetEsClient(configName)
	if err != nil {
		return "", err
	}

	return cli.InsertDoc(ctx, dataIndex, dataId, data)
}

//EsGetDoc读数据项
//ctx: context
//configName: es配置项名称
//dataIndex: 数据index
//dataId: 数据id
func EsGetDoc(ctx context.Context, configName string, dataIndex string, dataId string) ([]byte, error) {
	cli, err := GetEsClient(configName)
	if err != nil {
		return nil, err
	}

	return cli.GetDoc(ctx, dataIndex, dataId)
}

//EsDeleteDoc删除数据项
//ctx: context
//configName: es配置项名称
//dataIndex: 数据index
//dataId: 数据id
func EsDeleteDoc(ctx context.Context, configName string, dataIndex string, dataId string) (bool, error) {
	cli, err := GetEsClient(configName)
	if err != nil {
		return false, err
	}

	return cli.DeleteDoc(ctx, dataIndex, dataId)
}

//EsTermSearch 搜索需要根据具体功能封装(cli.GetConn())，这里只给一个举例
//ctx: context
//configName: es配置项名称
//dataIndex: 数据index
//term: term查询条件
func EsTermSearch(ctx context.Context, configName string, dataIndex string, term *elastic.TermQuery) (*elastic.SearchResult, error) {
	cli, err := GetEsClient(configName)
	if err != nil {
		return nil, err
	}

	result, err := cli.GetConn().Search().
		Index(dataIndex).
		Query(term).
		//Sort("id", true). //按id升序
		From(0).Size(10). //前10
		Pretty(true).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return result, nil
}
