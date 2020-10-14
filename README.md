# elastic client组件
## 1.组件描述
组件封装了elasticsearch客户端，实现了index创建，doc增加、获取、删除，以及term搜索，更多的搜索功能需要根据具体业务去实现。
## 2.如何使用
```
import (
    "github.com/airingone/config"
    "github.com/airingone/log"
    elasticclient "github.com/airingone/air-elasticclient"
)

func main() {
    config.InitConfig()                     //进程启动时调用一次初始化配置文件，配置文件名为config.yml，目录路径为../conf/或./
    log.InitLog(config.GetLogConfig("log")) //进程启动时调用一次初始化日志
    InitEsClient("elastic_test")    //初始化es client
    defer CloseEsClient()

    index := "index04"
    //创建索引,可以写doc时自动生成(但自动生成的会比较多其他默认字段)
    err := elasticclient.EsCreateIndex(context.Background(), "elastic_test", index, exampleMapping)
    log.Error("EsCreateIndex: err: %+v", err)

    //写数据doc
    var data exampleData
    data.Userid = "user01"
    data.Username = "username01"
    data.Userdesc = "user01"
    data.Usertel = 137
    id, err := elasticclient.EsInsertDoc(context.Background(), "elastic_test", index, "1", data)
    log.Error("EsInsertDoc: id: %s, err: %+v", id, err)

    //读doc
    data1, err := elasticclient.EsGetDoc(context.Background(), "elastic_test", index, "1")
    log.Error("EsGetDoc: data: %s, err: %+v", data1, err)

    //or
    //write doc
    esConfig := config.GetEsConfig("elastic_test")
    cli, err := NewEsClient(esConfig.Addr, esConfig.UserName, esConfig.Password, esConfig.TimeOutMs)
    if err != nil {
        log.Error("new es client err, err: %+v", err)
    return 
    }
    id2, err :=cli.InsertDoc(context.Background(), index, "1", data)
    log.Error("EsInsertDoc: id2: %s, err: %+v", id2, err)
    cli.Close()
}
```