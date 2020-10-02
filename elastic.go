package air_elasticclient
//es client封装
//注意对应olivere/elastic的版本应该与es的版本对应，这里目前使用的是v7版本

import (
	"context"
	"errors"
	"github.com/airingone/config"
	"github.com/airingone/log"
	"github.com/olivere/elastic/v7"
	"sync"
)

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
		cli, err := NewEsClient(config)
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

//es client
type EsClient struct {
	conn *elastic.Client   //es client
	config config.ConfigEs //config
}

//new es client
//config: es client配置
func NewEsClient(config config.ConfigEs) (*EsClient, error) {
	if len(config.Addr) == 0 {
		return nil, errors.New("addrs is empty")
	}
	//创建es client，维持长链接，并是协程安全的，如果无用户名与密码则直接为空即可
	client, err := elastic.NewClient(elastic.SetURL(config.Addr),
		elastic.SetBasicAuth(config.UserName, config.Password))
	if err != nil {
		return nil, err
	}
	_, _, err = client.Ping(config.Addr).Do(context.Background())
	if err != nil {
		return nil, err
	}

	cli := &EsClient{
		conn:client,
		config:config,
	}

	return cli, nil
}

//client close
func (cli *EsClient) Close() {
	cli.conn.Stop()
}

//conn get
func (cli *EsClient) GetConn() *elastic.Client {
	return cli.conn
}

//CreateIndex创建索引,也可以第一次写doc的时候自动生成索引
//ctx: context
//dataIndex: index
//mapping: 数据内容格式，定义为json结构体
func (cli *EsClient) CreateIndex(ctx context.Context, indexName string, mapping string) error {
	exist, err := cli.GetConn().IndexExists(indexName).Do(ctx)
	if err != nil {
		return err
	}
	if exist {
		return errors.New("index is exist")
	}

	_, err = cli.GetConn().CreateIndex(indexName).
		Body(mapping).
		Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

//InsertDoc写数据项（写入为json）,指定id创建数据
//ctx: context
//dataIndex: 数据index
//dataId: 数据id
//data: 数据内容，写入到Es为json格式，data数据可以是struct json或string json
func (cli *EsClient) InsertDoc(ctx context.Context, dataIndex string, dataId string, data interface{}) (string, error) {
	put, err := cli.GetConn().Index().
		Index(dataIndex).
		Id(dataId).
		BodyJson(data).
		//Refresh("wait_for").
		Do(ctx)
	if err != nil {
		return "", err
	}

	return put.Id, nil
}

//GetDoc读数据项
//ctx: context
//dataIndex: 数据index
//dataId: 数据id
func (cli *EsClient) GetDoc(ctx context.Context, dataIndex string, dataId string) ([]byte, error) {
	result, err := cli.GetConn().Get().
		Index(dataIndex).
		Id(dataId).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return result.Source, nil
}

//DeleteDoc删除数据项
//ctx: context
//dataIndex: 数据index
//dataId: 数据id
func (cli *EsClient) DeleteDoc(ctx context.Context, dataIndex string, dataId string) (bool, error) {
	result, err := cli.GetConn().Delete().
		Index(dataIndex).
		Id(dataId).
		Refresh("wait_for").
		Do(ctx)
	if err != nil {
		return false, err
	}
	if result.Result == "deleted" {
		return true, nil
	}

	return false, errors.New("delete err")
}