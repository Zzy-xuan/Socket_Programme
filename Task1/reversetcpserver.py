#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TCP Server - 支持多客户端并发处理的服务端程序
"""

# 模块导入
import socket          # socket模块：提供TCP/UDP网络通信功能
import struct          # struct模块：用于二进制数据的打包和解包
import threading       # threading模块：实现多线程并发处理
import time            # time模块：时间相关功能
import os              # os模块：操作系统相关功能（文件操作等）
from datetime import datetime  # datetime模块：获取精确时间戳

# 报文类型定义
"""
报文类型定义（Type字段，2字节）
"""
TYPE_INITIALIZATION = 1    # 初始化报文
TYPE_AGREE = 2             # 同意报文
TYPE_REVERSE_REQUEST = 3   # 反转请求报文
TYPE_REVERSE_ANSWER = 4    # 反转应答报文

# 日志记录模块
class Logger:
    """
    日志记录类 - 记录所有报文的发送与接收事件
    """
    
    def __init__(self, log_file='run_log.txt'):
        """
        初始化方法，在创建实例时自动调用
        参数：log_file（str）日志文件名，默认值为'run_log.txt'
        """
        self.log_file = log_file
        self.lock = threading.Lock()  # 线程锁，保证多线程写入安全
        
        # 如果日志文件已存在，先删除（每次运行生成全新日志）
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
    
    def log(self, event_type, message, client_addr=None):
        """
        记录日志事件
        参数:
            event_type (str): 事件类型（INFO/SEND/RECV/ERROR）
            message (str): 日志消息内容
            client_addr (tuple): 客户端地址 (IP, Port)，可选参数，默认值为None
        """
        with self.lock:  # 获取线程锁，保证写入安全
                         #%f: 微秒，[:-3] 截取字符串，去掉最后3位（微秒的后3位），保留毫秒
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # 根据是否有客户端地址，格式化不同的日志条目
            if client_addr:
                log_entry = f"[{timestamp}] [{event_type}] Client {client_addr}: {message}\n"
            else:
                log_entry = f"[{timestamp}] [{event_type}] {message}\n"
            
            # 写入日志文件（使用GBK编码，否则中文显示乱码）
            with open(self.log_file, 'a', encoding='gbk') as f: # 'a'模式：追加写入，不覆盖已有内容
                f.write(log_entry)
            
            print(log_entry.strip())

# 创建全局日志记录器实例（整个程序共用一个实例）
logger = Logger('run_log.txt')

"""
报文封装与解析模块
"""

def create_agree_packet():
    """
    创建Agree报文
    """
    # 使用struct打包，'!H'表示大端序的无符号短整型(2字节)
    packet = struct.pack('!H', TYPE_AGREE)
    return packet

def parse_initialization_packet(data):
    """
    解析Initialization报文
    参数:data (bytes): 接收到的字节数据（至少6字节）
    """
    #解析报文头部
    type_val, N = struct.unpack('!HI', data)
    
    # 验证报文类型是否正确
    if type_val != TYPE_INITIALIZATION:
        raise ValueError(f"报文类型错误: 期望{TYPE_INITIALIZATION}, 实际{type_val}")
    
    return N

def parse_reverse_request_packet(data):
    """
    解析reverseRequest报文
    参数:data (bytes): 接收到的完整报文数据
    """
    # 解析报文头部：type(2B) + Length(4B)
    type_val, length = struct.unpack('!HI', data[:6])
    
    # 验证报文类型是否正确
    if type_val != TYPE_REVERSE_REQUEST:
        raise ValueError(f"报文类型错误: 期望{TYPE_REVERSE_REQUEST}, 实际{type_val}")
    
    # 提取数据部分：从第7字节(索引6)开始，取length字节
    text_data = data[6:6+length]
    
    return text_data

def create_reverse_answer_packet(reversed_data):
    """
    创建reverseAnswer报文
    参数: reversed_data (bytes): 反转后的数据
    """
    length = len(reversed_data)
    # 打包：type(2B) + Length(4B) + reversed_data
    # struct.pack返回字节串，使用+连接反转数据
    packet = struct.pack('!HI', TYPE_REVERSE_ANSWER, length) + reversed_data
    return packet

# 客户端处理线程 
def handle_client(client_socket, client_addr):
    """
    客户端处理函数 - 处理单个客户端的完整通信流程
    参数:
        client_socket (socket.socket): 客户端socket对象，用于通信
        client_addr (tuple): 客户端地址，格式为 (IP字符串, Port整数)
    """
    logger.log("INFO", f"新客户端连接", client_addr)
    
    try:
        # 步骤1：接收Initialization报文 
        # 先接收6字节（type 2B + N 4B）
        init_header = client_socket.recv(6)
        
        # 检查接收是否成功
        if not init_header or len(init_header) < 6:
            logger.log("ERROR", "接收Initialization报文失败", client_addr)
            return
        
        # 解析Initialization报文，获取块数N
        N = parse_initialization_packet(init_header)
        logger.log("RECV", f"Initialization报文: N={N} (共{N}块待反转)", client_addr)
        
        # 步骤2：发送Agree报文
        agree_packet = create_agree_packet()
        client_socket.send(agree_packet)
        logger.log("SEND", f"Agree报文: 同意处理{N}块数据", client_addr)
        
        # 步骤3：循环处理N个reverseRequest报文
        for i in range(N):
            # （1）接收reverseRequest报文头部
            request_header = client_socket.recv(6)
            if not request_header or len(request_header) < 6:
                logger.log("ERROR", f"接收第{i+1}块reverseRequest报文头部失败", client_addr)
                break
            
            # 解析头部获取数据长度
            type_val, length = struct.unpack('!HI', request_header)
            
            # 验证报文类型
            if type_val != TYPE_REVERSE_REQUEST:
                logger.log("ERROR", f"报文类型错误: 期望{TYPE_REVERSE_REQUEST}, 实际{type_val}", client_addr)
                break
            
            # （2）接收数据部分
            data_received = b''  
            # 这里不直接使用recv(length)，因为recv可能不会一次性返回所有数据。就是一次没读完！
            # 所以需要循环接收直到收到完整的length字节
            while len(data_received) < length:
                # 每次尝试接收剩余需要的字节数
                chunk = client_socket.recv(length - len(data_received))
                if not chunk:  # 连接断开
                    break
                data_received += chunk
            
            logger.log("RECV", f"reverseRequest报文: 第{i+1}块, 长度={length}字节", client_addr)
            
            # （4）反转数据并发送reverseAnswer报文  
            try:
                text = data_received.decode('ascii')
                reversed_text = text[::-1]  # 字符串反转
                reversed_data = reversed_text.encode('ascii')
            except Exception as e:
                logger.log("ERROR", f"数据反转失败: {e}", client_addr)
                break
            
            # 创建并发送reverseAnswer报文
            answer_packet = create_reverse_answer_packet(reversed_data)
            client_socket.send(answer_packet)
            logger.log("SEND", f"reverseAnswer报文: 第{i+1}块反转完成, 长度={len(reversed_data)}字节", client_addr)
        
        logger.log("INFO", f"客户端处理完成，共处理{N}块数据", client_addr)
        
    except Exception as e:
        # 捕获所有异常，记录错误日志
        logger.log("ERROR", f"处理客户端请求时发生错误: {e}", client_addr)
    finally:
        # finally块确保无论是否发生异常，都会关闭socket
        client_socket.close()
        logger.log("INFO", f"客户端连接关闭", client_addr)

# 主服务器函数
def start_server(server_ip, server_port):
    """
    主服务器函数 - 启动TCP服务器并监听客户端连接
    """
    # 创建TCP socket

    #socket.AF_INET: IPv4地址族
    #socket.SOCK_STREAM: TCP流式套接字
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 设置socket选项

    #socket.SOL_SOCKET: socket级别选项
    #socket.SO_REUSEADDR: 允许地址重用选项
    #1: 启用该选项
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        # 绑定地址和端口
        server_socket.bind((server_ip, server_port))
        
        #listen参数backlog=5: 最大等待连接数
        # 表示系统允许最多5个客户端在等待队列中
        server_socket.listen(5)
        
        # 打印服务器启动信息
        print(f"{'='*60}")
        print(f"TCP Server 启动成功!")
        print(f"监听地址: {server_ip}:{server_port}")
        print(f"支持多客户端并发处理")
        print(f"{'='*60}\n")
        
        logger.log("INFO", f"服务器启动，监听 {server_ip}:{server_port}")
        
        # 主循环：持续接受客户端连接
        while True:
            # 接受客户端连接（阻塞等待）
            """
            accept()返回两个值：
            - client_socket: 新的socket对象，用于与该客户端通信
            - client_addr: 客户端地址元组 (IP, Port)
            """
            client_socket, client_addr = server_socket.accept()
            
            # 为每个客户端创建独立线程处理（实现多客户端并发）
            """
            threading.Thread参数：
            - target: 线程执行的函数
            - args: 函数参数（元组形式）
              总结一下就是告诉线程去启动哪个函数以及传入哪些参数
            - daemon=True: 设置为守护线程
            """
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_addr)
            )
            client_thread.daemon = True  # 设置为守护线程：主线程结束时自动终止
            client_thread.start()  # 启动线程
            
    except KeyboardInterrupt:
        # 用户按下Ctrl+C时关闭服务器
        print("\n服务器正在关闭...")
        logger.log("INFO", "服务器关闭")
    except OSError as e:
        # 端口绑定失败（通常是端口已被占用）
        print(f"错误: 无法绑定到端口 {server_port}，端口可能已被占用")
        logger.log("ERROR", f"端口绑定失败: {e}")
    except Exception as e:
        # 捕获其他异常
        print(f"服务器错误: {e}")
        logger.log("ERROR", f"服务器错误: {e}")
    finally:
        # finally块确保socket关闭
        server_socket.close()

# 程序入口 
if __name__ == '__main__':
    """
    程序入口 - 解析命令行参数并启动服务器    
    命令行参数格式：
        python server.py <server_ip> <server_port>
    """
    import sys
    
    # 命令行参数检查：必须有3个参数（程序名 + server_ip + server_port）
    if len(sys.argv) != 3:
        print("使用方法: python server.py <server_ip> <server_port>")
        print("示例: python server.py 127.0.0.1 8888")
        sys.exit(1)  # 参数错误，退出程序
    
    # 解析命令行参数
    server_ip = sys.argv[1]       # 获取服务器IP
    server_port = int(sys.argv[2]) # 获取服务器端口（转换为整数）
    
    # 启动服务器
    start_server(server_ip, server_port)