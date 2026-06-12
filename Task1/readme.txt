Task 1 TCP Socket programming

项目简介：
本程序实现了一个基于TCP的文本反转服务系统。客户端将ASCII文件随
机分块后发送给服务器，服务器将每块字符串反转后返回，客户端最终输
出完整的反转文件。

一．运行环境：
系统要求：
操作系统：Windows 10/Windows 11
Python版本：Python 3.13.3
依赖库：仅使用Python标准库，无需额外安装

网络环境要求：
客户端和服务器需要在同一网络中，或能够通过网络互相访问
支持IPv4地址
需要开放的TCP端口（建议使用10000以上的端口避免冲突）

文件系统要求：
输入文件必须为ASCII编码，最大限制10MB；
日志文件使用GBK编码

 二、配置选项：

1. 服务端配置：
   配置项：server_ip，类型：字符串，默认值：无
   配置项：server_port，类型：整数，默认值：无
   配置项：backlog，类型：整数，默认值：5
   配置项：log_file，类型：字符串，默认值："run_log.txt"
   配置项：SO_REUSEADDR，类型：布尔值，默认值：True

2. 客户端配置：
   配置项：server_ip，类型：字符串，默认值：无
   配置项：server_port，类型：整数，默认值：无
   配置项：file_path，类型：字符串，默认值：无
   配置项：Lmin，类型：整数，默认值：42（可选）
   配置项：Lmax，类型：整数，默认值：无
   配置项：seed，类型：整数，默认值：无
   配置项：log_file，类型：字符串，默认值："run_log.txt"
   配置项：MAX_FILE_SIZE，类型：整数，默认值：10*1024*1024
   配置项：MAX_PACKET_LENGTH，类型：整数，默认值：10000
   配置项：timeout_min，类型：浮点数，默认值：5.0秒
   配置项：timeout_max，类型：浮点数，默认值：10.0秒


3.报文类型常量（不可配置，仅供参考）
常量名	                                  值	说明
TYPE_INITIALIZATION	1	初始化类型报文
TYPE_AGREE	                 2	同意类型报文
TYPE_REVERSE_REQUEST	3	反转请求类型报文
TYPE_REVERSE_ANSWER	4	反转应答类型报文

三.  安装说明
1. 克隆仓库
```bash
git clone https://github.com/Zzy_xuan/socket-programme.git
cd tcp-socket-program/TCP
```

2. 验证Python版本
```bash
python --version
# 应显示 Python 3.6 或更高版本
```

3. 准备测试文件
创建一个ASCII编码的测试文件（全英文可打印字符）：
```bash
echo "Hello World! This is a test file for TCP socket program." > test.txt
```

---

四.  使用方法

1.启动服务器
在服务器端（Guest OS）运行：

```bash
python server.py <server_ip> <server_port>
```

[示例]：
```bash
# 监听所有网卡
python server.py 0.0.0.0 8888

# 仅监听本机
python server.py 127.0.0.1 8888
```

[输出]：
```
============================================================
TCP Server 启动成功!
监听地址: 0.0.0.0:8888
支持多客户端并发处理
============================================================
```

2.启动客户端
在客户端（Host OS）运行：

```bash
python client.py <server_ip> <server_port> <file_path> <Lmin> <Lmax> [seed]
```

[示例]：
```bash
# 使用默认随机种子（42）
python client.py 192.168.1.100 8888 test.txt 50 100

# 指定随机种子
python client.py 192.168.1.100 8888 test.txt 50 100 42
```

[输出]：
```
============================================================
TCP Client 启动
服务器地址: 192.168.1.100:8888
文件路径: test.txt
分块范围: [50, 100] 字节
随机种子: 42
============================================================

分块详情:
第1块: 73字节, 起始位置: 0
第2块: 91字节, 起始位置: 73
...

第1块: reversed text here...
第2块: another reversed text...
...

最终反转结果已保存到: test_reversed.txt
```

---

五.  文件结构

```
240801112周雨萱/Task1/
├── readme.txt                             # 本说明文档
├── reservetcpclient.py               # 客户端主程序
├── reservetcpserver.py              # 服务器主程序
├── run_log.txt                             # 运行日志（示例，运行后生成）
├── tcp_packet_capture.doc       # wireshark抓包截图，运行终端截图，关键代码等说明文档
├── test.txt                                   # 测试文件（示例）
└── test_reversed.txt                   # 最终输出的反转文本（示例，运行后生成）
```

六. 代码框架
reversetcpclient.py
│
├── 导入模块（socket, struct, random, datetime, os）
│
├── 常量定义
│   ├── TYPE_INITIALIZATION = 1
│   ├── TYPE_AGREE = 2
│   ├── TYPE_REVERSE_REQUEST = 3
│   └── TYPE_REVERSE_ANSWER = 4
│
├── Logger类（日志记录）
│   ├── __init__(): 初始化
│   └── log(): 记录日志
│
├── 分块算法函数【验收重点】
│   └── calculate_chunks(): 计算分块方案
│
├── 报文封装与解析函数
│   ├── create_initialization_packet(): 创建Initialization报文
│   ├── parse_agree_packet(): 解析Agree报文
│   ├── create_reverse_request_packet(): 创建reverseRequest报文
│   └── parse_reverse_answer_packet(): 解析reverseAnswer报文
│
├── 主客户端函数
│   └── start_client(): 执行完整通信流程
│
└── 程序入口
    └── if __name__ == '__main__': 解析参数并启动
```

reversetcpserver.py
│
├── 导入模块（socket, struct, threading, datetime, os）
│
├── 常量定义
│   ├── TYPE_INITIALIZATION = 1
│   ├── TYPE_AGREE = 2
│   ├── TYPE_REVERSE_REQUEST = 3
│   └── TYPE_REVERSE_ANSWER = 4
│
├── Logger类（日志记录）
│   ├── __init__(): 初始化
│   └── log(): 记录日志
│
├── 报文封装与解析函数
│   ├── create_agree_packet(): 创建Agree报文
│   ├── parse_initialization_packet(): 解析Initialization报文
│   ├── parse_reverse_request_packet(): 解析reverseRequest报文
│   └── create_reverse_answer_packet(): 创建reverseAnswer报文
│
├── 客户端处理函数
│   └── handle_client(): 处理单个客户端
│
├── 主服务器函数
│   └── start_server(): 启动服务器
│
└── 程序入口
    └── if __name__ == '__main__': 解析参数并启动
```