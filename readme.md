## logstream
基于本地文件系统+内存实现的轻量级高性能读写分离流式事件队列框架, 用于在性能苛刻场景下对事件数据进行异步持久化/异步流式+批量消费的应场景。
框架采用读写分离的方式将事件数据以流形式写入持久化,并以流+批形式传输给消费端,数据写入逻辑和消费逻辑可解耦在不同的进程中实现。事件流根据
topic进行分发,消费端可以实时配置topic黑名单/白名单自定义对数据进行过滤消费,
被过滤的topic数据不会丢失,会在开启白名单或移除黑名单后从上次已完成消费消息位移开始消费.

使用场景:

1.充当可跨进程的本地化异步队列使用,不需要安装任何第三方服务(如最常见的redis)作为依赖,即可快速简单实现基于topic分发消息的异步队列。

2.如大型网络游戏服务器需要对大量事件进行海量数据埋点采集进行BI数据分析,需考虑大量数据埋点带来网络IO阻塞可能对游戏服务器实时性带来的影响。

3.微服务架构中rpc请求埋点日志。

4.分布式链路日志采集系统。

5.大流量服务器接口请求埋点分析。

等等...

## install
go get github.com/995933447/logstream

## usage

step1:设置框架配置文件，非windows:/etc/logstream/meta.json 或 windows: C:\logstream\meta.json
````
{
  "base_dir": "D:\\log", // 事件数据持久化目录
  "idx_file_max_item_num": 1000000, // 单个文件可存储事件最大可容纳消息数量，超过则切换新文件
  "data_file_max_size": "1G", // 单个文件可存储消息最大容量，超过则切换新文件
  "mem_max_size": "200M", // 消费端程序最大可占用系统内存(保护系统,最大可用内存越大,同时被并发采集事件消费的topic越多,效率越高)
  "max_concurrent_forward": 100, // 消费端程序回调处理消息最大worker数
  "compress_topics": [], // 压缩消息存储的topic,可节持久化目录省磁盘空间
  "black_topics": [], // 黑名单topic,值不为空的话,黑名单内的topic不会被消费
  "white_topics": [] // 白名单topic,值不为空的话,白名单的topic才会被消费
}
````

step2:进行数据处理：
````
1.写入端方法示例：
package logstream

import (
	"fmt"
	"testing"
)

func TestWriter_Write(t *testing.T) {
    // new 一个写入器
	writer, _ := NewWriter("")
	for i := 0; i < 10; i++ {
	    // 写入10条数据到test_topic的topic下
		writer.Write("test_topic", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
}

func TestWriter_Flush(t *testing.T) {
	writer, err := NewWriter("")
	if err != nil {
		t.Log(err)
		return
	}
	for i := 0; i < 2; i++ {
		writer.Write("test_topic2", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	// 把write的数据同步刷盘(默认刷盘是异步进行的)
	// 一般情况下让数据异步刷入磁盘即可,除非进程突然推出
	writer.Flush()
}

func TestWriter_Exit(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 1000; i++ {
		writer.Write("test_topic3", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	// 退出并把write的数据同步刷盘(默认刷盘是异步进行的),退出后释放对象资源,并且无法恢复
	writer.Exit()
}

func TestWriter_Stop(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 10000; i++ {
		writer.Write("test_topic4", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	// 停止并把write的数据同步刷盘(默认刷盘是异步进行的),停止后将暂时是否对象资源并且无法写入数据
	writer.Stop()
}

func TestWriter_Resume(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 10000; i++ {
		writer.Write("test_topic5", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Stop()
	// writer恢复工作,可继续写入数据
	err := writer.Resume()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		writer.Write("test_topic5", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Exit()
	
	// 退出后无法恢复消费，需要开启新的writer，所这里会返回错误errWriterExited
	err = writer.Resume()
	if err != nil {
		t.Fatal(err)
	}
}

````
````
2.数据消费端方法示例：
var (
	readStream *Reader
	err        error
)
// 配置消费处理逻辑
readStream, err = NewReader("", func(items []*PoppedMsgItem) error {
	// 一次处理回调中会只处理一个topic中的一批事件数据(异步读取历史事件数据,并监听实时产生的事件数据)
	// 一批次消息最大不超过2M
	fmt.Println("consume " + items[0].Topic)
	for _, item := range items {
		fmt.Println(string(item.Data))
		// 确认消息已完成,消费逻辑是串行消费的,
		// 确认消费完才会消费下一批
		readStream.ConfirmMsg(item.Topic, item.Seq, item.IdxOffset)
	}
	fmt.Println(items[0].Topic+" batch consumed", len(items))
	// 如果在消费过程中返回err,说明消费失败,这批消息会进行重试
	return nil
})
if err != nil {
	panic(err)
}
// 开始消费
readStream.Start()
````

api 总结:

writer: 
````
异步写入数据
func (w *Writer) Write(topic string, buf []byte)

把write的数据同步刷盘(默认刷盘是异步进行的),一般情况下让数据异步刷入磁盘即可,除非进程突然推出
func (w *Writer) Flush()

把write的数据同步刷盘(默认刷盘是异步进行的),并停止接受写入(这里会释放一些内存资源),继续写入会发生阻塞,直到resume
func (w *Writer) Stop()

恢复已经stopped的writer,继续接受write
func (w *Writer) Resume() err

把write的数据同步刷盘(默认刷盘是异步进行的),并退出接受写入(完全释放内存资源),继续写入会发生阻塞,无法resume
func (w *Writer) Exit() 
````
reader:
````
创建一个读取器,cfgFilePath不指定默认去非windows:/etc/logstream/meta.json 或 
windows: C:\logstream\meta.json读取配置,forwarder用于消费处理时间数据,forward
接口定义为func(items []*PoppedMsgItem) error
NewReader(cfgFilePath string, forwarder ForwardFunc) (*Reader, error)

确认消息已完成
func (*Reader) ConfirmMsg()

开始消费线程
func (*Reader) Start()
````