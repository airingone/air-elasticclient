package air_elasticclient

//es client封装
//注意对应olivere/elastic的版本应该与es的版本对应，这里目前使用的是v7版本

import (
	"context"
	"errors"
	"github.com/olivere/elastic/v7"
)

//es client
type EsClient struct {
	conn      *elastic.Client //es client
	Addr      string          //地址，可为: "http://127.0.0.1:9200"
	UserName  string          //用户名，如果无用户名与密码则直接为空即可
	Password  string          //用户密码，如果无用户名与密码则直接为空即可
	TimeOutMs uint32          //请求耗时
}

//new es client
//config: es client配置
func NewEsClient(addr string, userName string, password string, timeOutMs uint32) (*EsClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("addrs is empty")
	}
	//创建es client，维持长链接，并是协程安全的，如果无用户名与密码则直接为空即可
	client, err := elastic.NewClient(elastic.SetURL(addr),
		elastic.SetBasicAuth(userName, password))
	if err != nil {
		return nil, err
	}
	_, _, err = client.Ping(addr).Do(context.Background())
	if err != nil {
		return nil, err
	}

	cli := &EsClient{
		conn:      client,
		Addr:      addr,
		UserName:  userName,
		Password:  password,
		TimeOutMs: timeOutMs,
	}

	return cli, nil
}

//client close
func (cli *EsClient) Close() {
	if cli.conn != nil {
		cli.conn.Stop()
	}
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
