#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TCP Client - 发送文件分块并接收反转结果的客户端程序
功能概述：
    1. 读取ASCII文件内容
    2. 根据Lmin、Lmax和seed参数进行随机分块
    3. 发送Initialization报文告知服务器块数N
    4. 逐块发送reverseRequest报文，接收reverseAnswer报文
    5. 在终端打印每块反转结果
    6. 输出最终反转后的文件
"""

# 导入必要的模块
import socket          # socket模块：提供TCP/UDP网络通信功能
import struct          # struct模块：用于二进制数据的打包和解包
import random          # random模块：生成随机数（用于分块）
import os              # os模块：操作系统相关功能（文件路径处理等）
from datetime import datetime  # datetime模块：获取精确时间戳

# 报文类型常量定义
"""
报文类型定义（Type字段，2字节）
"""
TYPE_INITIALIZATION = 1    # 初始化报文类型标识
TYPE_AGREE = 2             # 同意报文类型标识
TYPE_REVERSE_REQUEST = 3   # 反转请求报文类型标识
TYPE_REVERSE_ANSWER = 4    # 反转应答报文类型标识

# 日志记录模块
class Logger:
    """
    日志记录类 - 记录所有报文的发送与接收事件
    事件类型：
        - INFO: 一般信息（连接、断开等）
        - SEND: 发送报文事件
        - RECV: 接收报文事件
        - ERROR: 错误事件
    """
    
    def __init__(self, log_file='run_log.txt'):
        """
        初始化日志记录器 
        参数:log_file (str): 日志文件名，默认为'run_log.txt'
        """
        self.log_file = log_file
        
        # 如果日志文件已存在，先删除（每次运行生成全新日志）
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
    
    def log(self, event_type, message):
        """
        记录日志事件
        参数:event_type (str): 事件类型（INFO/SEND/RECV/ERROR）
            message (str): 日志消息内容
        """
        # 生成时间戳：精确到毫秒
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        log_entry = f"[{timestamp}] [{event_type}] {message}\n"
        
        # 写入日志文件（使用GBK编码）
        with open(self.log_file, 'a', encoding='gbk') as f:
            f.write(log_entry)
        
        # 同时打印到控制台
        print(log_entry.strip())

# 创建全局日志记录器实例（整个程序共用一个实例）
logger = Logger('run_log.txt')

# 分块算法函数
def calculate_chunks(file_size, Lmin, Lmax, seed):
    """
    计算文件分块
    
    参数:
        file_size (int): 文件总字节数
        Lmin (int): 每块最小字节数
        Lmax (int): 每块最大字节数
        seed (int): 随机种子（保证分块方案可重现）
    
    返回:
        list: 各块长度列表，如 [73, 91, 58, ...]
    """
    # 设置随机种子（保证可重现）
    random.seed(seed)
    
    chunks = []       # 存储各块长度的列表
    remaining = file_size  # 剩余未分配的字节数
    
    # 循环分配各块长度
    while remaining > 0:
        if remaining <= Lmax: # 剩余字节小于等于Lmax，最后一块取剩余所有
                              # 最后一块长度可以小于Lmin
            chunks.append(remaining)
            remaining = 0
        else:
            chunk_size = random.randint(Lmin, Lmax) # 生成[Lmin, Lmax]范围内的随机长度
            chunks.append(chunk_size)
            remaining -= chunk_size
    
    return chunks


"""
报文封装与解析模块
"""

def create_initialization_packet(N):
    """
    创建Initialization报文
    参数:N (int): 块数（数据分块的总数量）
    返回: bytes: 封装好的Initialization报文字节流
    """

    packet = struct.pack('!HI', TYPE_INITIALIZATION, N)    # 打包：type(2B) + N(4B)
    return packet

def parse_agree_packet(data):
    """
    解析Agree报文
    参数:data (bytes): 接收到的字节数据（2字节）
    返回: bool: True表示解析成功
    """
    type_val = struct.unpack('!H', data)[0]    # 解包2字节获取Type字段，struct.unpack返回元组
                                               # 元组只有一个元素，所以用[0]取第一个元素
    
    if type_val != TYPE_AGREE:    # 验证报文类型是否正确
        raise ValueError(f"报文类型错误: 期望{TYPE_AGREE}, 实际{type_val}")
    
    return True

def create_reverse_request_packet(data):
    """
    创建reverseRequest报文
    参数:data (bytes): 要发送的数据块
    返回: bytes: 封装好的reverseRequest报文字节流
    """
    length = len(data)
    # 打包：type(2B) + Length(4B) + data
    packet = struct.pack('!HI', TYPE_REVERSE_REQUEST, length) + data # struct.pack返回字节串，使用+连接数据部分
    return packet

def parse_reverse_answer_packet(data):
    """
    解析reverseAnswer报文
    参数:data (bytes): 接收到的完整报文数据
    返回: bytes: 反转后的数据部分
    """
    type_val, length = struct.unpack('!HI', data[:6])    # 解析头部：type(2B) + Length(4B)
    
    if type_val != TYPE_REVERSE_ANSWER:    # 验证报文类型是否正确
        raise ValueError(f"报文类型错误: 期望{TYPE_REVERSE_ANSWER}, 实际{type_val}")
    
    reversed_data = data[6:6+length]# 提取反转后的数据：从第7字节(索引6)开始，取length字节
    
    return reversed_data

# 主客户端函数
def start_client(server_ip, server_port, file_path, Lmin, Lmax, seed):
    """
    主客户端函数 - 执行完整的客户端通信流程
    """
    # 步骤1：读取文件
    print(f"{'='*60}")
    print(f"TCP Client 启动")
    print(f"服务器地址: {server_ip}:{server_port}")
    print(f"文件路径: {file_path}")
    print(f"分块范围: [{Lmin}, {Lmax}] 字节")
    print(f"随机种子: {seed}")
    print(f"{'='*60}\n")
    
    # 1.1处理文件路径：支持相对路径和绝对路径
    import sys
    if not os.path.isabs(file_path): # 如果不是绝对路径
        # 先尝试当前工作目录
        if os.path.exists(file_path):
            actual_path = file_path
        else:
            # 再尝试脚本所在目录
            #sys.argv[0] 是脚本文件名，os.path.dirname() 获取脚本所在目录
            #os.path.abspath() 确保路径是绝对路径
            script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            actual_path = os.path.join(script_dir, file_path) #拼接脚本所在目录和相对路径，得到绝对路径
            if not os.path.exists(actual_path):
                print(f"错误: 文件不存在")
                return
    else: # 如果是绝对路径
        actual_path = file_path
        if not os.path.exists(actual_path):
            print(f"错误: 文件 {actual_path} 不存在")
            return
    
    # 1.2读取文件内容（ASCII编码）
    try:
        with open(actual_path, 'r', encoding='ascii') as f:
            file_content = f.read()
    except Exception as e:
        print(f"错误: 读取文件失败 - {e}")
        return
    
    # 1.3将文件内容编码为字节串
    file_data = file_content.encode('ascii')
    file_size = len(file_data)
    
    # 1.4文件大小限制验证：防止文件过大导致内存或传输问题
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB上限（10 * 1024 * 1024字节）
    if file_size > MAX_FILE_SIZE:
        print(f"错误: 文件过大，超过{MAX_FILE_SIZE/1024/1024}MB限制")
        print(f"  当前文件大小: {file_size/1024/1024:.2f}MB")
        logger.log("ERROR", f"文件过大: {file_size}字节，超过{MAX_FILE_SIZE}字节限制")
        return
    
    logger.log("INFO", f"读取文件成功: {actual_path}, 大小: {file_size}字节")
    
    # 步骤2：计算分块方案
    chunks = calculate_chunks(file_size, Lmin, Lmax, seed)
    N = len(chunks)
    
    logger.log("INFO", f"分块方案: 共{N}块, 各块长度: {chunks}")
    
    # 打印分块详情
    print(f"\n分块详情:")
    cumulative = 0  # 累计已分配的字节数
    #chunks是一个列表，每个元素是当前块的长度
    #enumerate()函数会同时获取列表的索引和元素值，且返回格式为(索引, 元素值)，索引在前
    #设定i从1开始，因为分块详情输出格式中是第1块、第2块等，而不是第0块、第1块等
    for i, chunk_size in enumerate(chunks, 1):
        print(f"  第{i}块: {chunk_size}字节, 起始位置: {cumulative}")
        cumulative += chunk_size
    print()
    
    # 步骤3：连接服务器
    #socket.AF_INET: IPv4地址族
    #socket.SOCK_STREAM: TCP流式套接字
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 连接超时设置：防止长时间等待无响应的服务器
    client_socket.settimeout(5.0)  # 设置连接超时时间为5秒
    
    try:
        # 连接服务器（可能抛出ConnectionRefusedError或socket.timeout）
        #connect参数：元组 (IP字符串, Port整数)
        client_socket.connect((server_ip, server_port))
        logger.log("INFO", f"成功连接到服务器 {server_ip}:{server_port}")
        
        # 连接成功后，设置数据传输超时时间（比连接超时更长）
        client_socket.settimeout(10.0)  # 设置数据传输超时时间为10秒
        
        # 步骤4：发送Initialization报文
        init_packet = create_initialization_packet(N)
        client_socket.send(init_packet)
        logger.log("SEND", f"Initialization报文: N={N} (共{N}块)")
        
        # 步骤5：接收Agree报文
        agree_data = client_socket.recv(2)
        
        # 接收数据完整性检查：确保接收到完整的报文头部
        if len(agree_data) < 2:
            logger.log("ERROR", "接收Agree报文失败：数据不完整或连接断开")
            print("错误: 接收Agree报文失败，服务器可能已断开连接")
            return
        
        # 解析并验证Agree报文
        parse_agree_packet(agree_data)
        logger.log("RECV", "Agree报文: 服务器同意处理")
        
        # 步骤6：循环发送reverseRequest并接收reverseAnswer
        reversed_results = []  # 存储所有反转后的结果
        offset = 0  # 当前文件偏移量（字节位置）
        
        for i in range(N):
            # 获取当前块的数据
            chunk_size = chunks[i]
            chunk_data = file_data[offset:offset+chunk_size] #使用切片从file_data中提取当前块的数据
            offset += chunk_size
            
            # 发送reverseRequest报文
            request_packet = create_reverse_request_packet(chunk_data)
            client_socket.send(request_packet)
            logger.log("SEND", f"reverseRequest报文: 第{i+1}块, 长度={chunk_size}字节")
            
            # 接收reverseAnswer报文头部（type 2B + Length 4B）
            answer_header = client_socket.recv(6)
            
            # 接收数据完整性检查：确保接收到完整的报文头部
            if len(answer_header) < 6:
                logger.log("ERROR", f"接收第{i+1}块reverseAnswer报文头部失败：数据不完整或连接断开")
                print(f"错误: 接收第{i+1}块报文头部失败，服务器可能已断开连接")
                break
            
            # 解析头部获取数据长度
            type_val, length = struct.unpack('!HI', answer_header)
            
            # 报文长度异常验证：防止恶意或错误的超长报文
            MAX_PACKET_LENGTH = 10000  # 设置报文数据长度上限为10000字节
            if length > MAX_PACKET_LENGTH:
                logger.log("ERROR", f"报文长度异常: 第{i+1}块长度{length}字节，超过{MAX_PACKET_LENGTH}字节上限")
                print(f"错误: 第{i+1}块报文长度异常，超过上限")
                break
            
            # 验证报文类型
            if type_val != TYPE_REVERSE_ANSWER:
                logger.log("ERROR", f"报文类型错误: 期望{TYPE_REVERSE_ANSWER}, 实际{type_val}")
                print(f"错误: 第{i+1}块报文类型错误")
                break
            
            # 接收数据部分
            answer_data = b''
            while len(answer_data) < length:
                chunk = client_socket.recv(length - len(answer_data)) #会接着之前接收的数据继续接收
                if not chunk:  # 连接断开
                    break
                answer_data += chunk
            
            # 接收数据完整性检查：确保接收到完整的数据部分
            if len(answer_data) < length:
                logger.log("ERROR", f"接收第{i+1}块数据不完整：期望{length}字节，实际接收{len(answer_data)}字节")
                print(f"错误: 第{i+1}块数据接收不完整")
                break
            
            # 解析reverseAnswer报文
            # 将头部和数据部分合并后解析
            reversed_data = parse_reverse_answer_packet(answer_header + answer_data)
            
            # 数据编码错误处理：确保数据能正确解码为ASCII
            try:
                reversed_text = reversed_data.decode('ascii')
            except UnicodeDecodeError as e:
                logger.log("ERROR", f"第{i+1}块数据解码失败: {e}")
                print(f"错误: 第{i+1}块数据包含非ASCII字符，无法解码")
                break
            #这里其实存在冗余，不用再走一遍解析函数，直接decode answer_data即可
            #但是这样写第一是保持封装性，第二是方便后续扩展，如果报文格式发生变化，只需要修改解析函数即可
            #而不需要修改其他代码
            
            logger.log("RECV", f"reverseAnswer报文: 第{i+1}块, 长度={len(reversed_data)}字节")
            
            # 步骤7：打印反转结果到终端
            print(f"第{i+1}块: {reversed_text}")
            
            # 存储反转结果（用于最终输出）
            reversed_results.append(reversed_text)
        
        # 步骤8：输出最终反转文件
        # 将所有反转后的块按顺序拼接
        # 最终结果应该是整个文件的反转
        final_result = ''.join(reversed_results)
        
        # 生成输出文件名
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        output_file = f"{base_name}_reversed.txt"
        
        # 写入反转文件（ASCII编码）
        with open(output_file, 'w', encoding='ascii') as f:
            f.write(final_result)
        
        logger.log("INFO", f"反转文件已保存: {output_file}")
        print(f"\n最终反转结果已保存到: {output_file}")
        
        # 打印原始文件的反转(用于对比)
        original_reversed = file_content[::-1]
        print(f"\n验证: 原始文件整体反转结果:")
        print(f"{original_reversed}")
        print(f"\n客户端处理完成!")
        
    except ConnectionRefusedError:
        # 连接被拒绝（服务器未启动或端口错误）
        logger.log("ERROR", f"无法连接到服务器 {server_ip}:{server_port}")
        print(f"错误: 无法连接到服务器，请确认服务器已启动")
    except Exception as e:
        # 捕获其他异常
        logger.log("ERROR", f"客户端错误: {e}")
        print(f"错误: {e}")
    finally:
        # finally块确保socket关闭
        client_socket.close()
        logger.log("INFO", "客户端连接关闭")

# 程序入口
if __name__ == '__main__':
    """
    程序入口 - 解析命令行参数并启动客户端
    """
    import sys
    
    # 命令行参数检查（支持5或6个参数）
    # len(sys.argv) == 6: 不带seed参数（使用默认值42）
    # len(sys.argv) == 7: 带seed参数
    if len(sys.argv) < 6 or len(sys.argv) > 7:
        print("使用方法: python client.py <server_ip> <server_port> <file_path> <Lmin> <Lmax> [seed]")
        print("示例: python client.py 127.0.0.1 8888 test.txt 50 100")
        print("      python client.py 127.0.0.1 8888 test.txt 50 100 42")
        print("\n说明:")
        print("  server_ip   - 服务器IP地址")
        print("  server_port - 服务器端口")
        print("  file_path   - 要发送的ASCII文件路径")
        print("  Lmin        - 每块最小字节数")
        print("  Lmax        - 每块最大字节数")
        print("  seed        - 随机种子（可选，默认42）")
        sys.exit(1)  # 参数错误，退出程序
    
    # 解析命令行参数
    server_ip = sys.argv[1]       # 获取服务器IP
    
    # 端口参数验证：确保能转换为整数
    try:
        server_port = int(sys.argv[2])  # 获取服务器端口（转换为整数）
    except ValueError:
        print("错误: 端口参数必须是整数")
        sys.exit(1)
    
    file_path = sys.argv[3]       # 获取文件路径
    
    # Lmin/Lmax参数验证：确保能转换为整数
    try:
        Lmin = int(sys.argv[4])   # 获取Lmin（转换为整数）
        Lmax = int(sys.argv[5])   # 获取Lmax（转换为整数）
    except ValueError:
        print("错误: Lmin和Lmax参数必须是整数")
        sys.exit(1)
    
    # 获取随机种子（可选参数，默认42）
    if len(sys.argv) == 7:
        try:
            seed = int(sys.argv[6])  # 使用命令行指定的seed
        except ValueError:
            print("错误: seed参数必须是整数")
            sys.exit(1)
    else:
        seed = 42  # 默认随机种子
    
    # 参数验证
    
    # IP地址格式验证：使用socket.inet_aton()验证IP格式是否正确
    try:
        socket.inet_aton(server_ip)  # 验证IP地址格式（支持IPv4）
    except socket.error:
        print("错误: IP地址格式无效")
        sys.exit(1)
    
    # 端口范围验证：端口必须在1-65535范围内（TCP/UDP端口范围）
    if server_port < 1 or server_port > 65535:
        print("错误: 端口必须在1-65535范围内")
        sys.exit(1)
    
    # Lmin/Lmax范围验证：必须大于0
    if Lmin <= 0 or Lmax <= 0:
        print("错误: Lmin和Lmax必须大于0")
        sys.exit(1)
    
    # Lmin/Lmax关系验证：Lmin不能大于Lmax
    if Lmin > Lmax:
        print("错误: Lmin不能大于Lmax")
        sys.exit(1)
    
    # 启动客户端
    start_client(server_ip, server_port, file_path, Lmin, Lmax, seed)