Task 2 UDP Socket programming

项目简介：
本项目在UDP不可靠传输的基础上，通过自定义应用层协议实现了类似TCP的可靠数据传输机制。主要功能包括：

三次握手连接建立：模拟TCP连接建立过程，包含身份验证机制
GBN滑动窗口协议：实现Go-Back-N协议，支持窗口内连续发送
超时重传机制：动态调整超时时间，自动重传丢失数据包
累积确认机制：服务器端采用累积确认方式响应
丢包模拟：应用层模拟网络丢包场景
RTT统计：使用pandas计算RTT统计信息

一．运行环境
系统要求：
操作系统：Windows 10/Windows 11
Python版本：Python 3.13.3
依赖库：除使用Python标准库，需额外安装panda库
```
bash
pip install pandas
```

网络环境要求：
客户端和服务器需要在同一网络中，或能够通过网络互相访问
支持IPv4地址
需要开放的UDP端口（建议使用10000以上的端口避免冲突）

二．配置选项
1.服务端配置：
参数	                 说明	                 默认值	是否必填
-p / --port	服务器监听端口	无	必填
-s / --student_id	学号后4位	                 无	必填
-l / --loss_rate	丢包率(0~1)	0.15	非必填

2.客户配置：
参数	                  说明	                        默认值	是否必填
-i / --ip	                  服务器IP地址	       无	                 必填
-p / --port	 服务器端口	       无	                 必填
-s / --student_id	 学号后四位	       无	                 必填
-t /--timeout	 初始超时时间	       0.3s/300ms	非必填
-n / --num_packets	 发送数据包数量	       30	                 非必填
-w / --window_size	 发送窗口大小（字节）    400	                 非必填

3.协议常量配置：
常量	                                  说明	                 默认值
DEFAULT_TIMEOUT	                 默认超时时间	0.3s/300ms
MIN_DATA_SIZE	                 最小数据包大小	40
MAX_DATA_SIZE	                 最大数据包大小	80
WINDOW_SIZE	                 发送窗口大小	400
TOTAL_PACKETS_TO_SEND	默认丢包率	0.15

三.  项目结构

```
240801112周雨萱/Task2/ 
                                 ├── logger.py                             # 日志工具
                                 ├── protocol.py                          #  协议定义
                                 ├── readme.txt                           #  本说明文档
                                 ├── run_log.txt                           #  客户端日志（此为示例。运行后生成）
                                 ├── run_log_server.txt               #  服务器日志（运行后生成）
                                 ├── udp_packet_capture.doc    #  抓包截图，运行截图，关键代码说明文档
                                 ├── udpclient.py                        #  客户端主程序
                                 └── udpserver.py                       #  服务器日志（此为示例。运行后生成）
```
四. 协议设计
1.报文类型
SYN                         type=1                   连接请求
SYN_ACK                type=2                   连接请求确认
ACK                         type=3                   确认
DATA                      type=4                   数据报文
DATA_ACK              type=5                  数据累计确认
FIN                           type=6                  连接释放请求
FIN_ACK                  type=7                  连接释放确认 

2.报文格式
┌─────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
│     类型(1B)    │      序列号(2B)      |      确认号(2B)     │     数据长度(2B)  │  StudentID(2B)   |      时间戳(2B)      │
└─────────┴───────────┴───────────┴───────────┴───────────┴───────────
五.  使用方法

1.启动服务器
```bash
python UDPserver.py -p <端口号> -s <学号后4位> [-l <丢包率>]
```

【示例】
```bash
# 基本用法
python server.py -p 8888 -s 1234

# 自定义丢包率
python server.py -p 8888 -s 1234 -l 0.20

# 低丢包率测试
python server.py -p 8888 -s 1234 -l 0.05
```

2.启动客户端
```bash
python client.py -i <服务器IP> -p <服务器端口> -s <学号后4位> [-t <超时>] [-n <包数>] [-w <窗口大小>]
```

【示例】
```bash
# 基本用法
python client.py -i 192.168.1.100 -p 8888 -s 1234

# 停等协议（窗口1个包）
python client.py -i 192.168.1.100 -p 8888 -s 1234 -w 40

# GBN协议（窗口5个包）
python client.py -i 192.168.1.100 -p 8888 -s 1234 -w 400

# 自定义参数
python client.py -i 192.168.1.100 -p 8888 -s 1234 -t 0.5 -n 50 -w 800
```