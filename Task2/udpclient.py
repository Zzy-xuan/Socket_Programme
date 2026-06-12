"""
UDP可靠传输 - 客户端
模拟TCP连接建立和可靠数据传输
实现GBN（Go-Back-N）滑动窗口协议
"""

import socket
import random
import time
import argparse
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd

from protocol import (
    Packet, PacketType, PacketHeader,
    HEADER_SIZE, DEFAULT_TIMEOUT, MIN_DATA_SIZE, MAX_DATA_SIZE,
    WINDOW_SIZE, TOTAL_PACKETS_TO_SEND,
    calculate_student_id, create_syn_packet, create_ack_packet,
    create_data_packet, create_fin_packet, get_current_time_str
)
from logger import Logger

# 默认窗口最大包数（基于400字节窗口和80字节包大小）
DEFAULT_MAX_PACKETS_IN_WINDOW = 10


@dataclass
class PacketInfo:
    """数据包信息"""
    seq_num: int           # 序列号
    data: bytes            # 数据内容
    data_start: int       # 数据起始字节偏移
    data_end: int          # 数据结束字节偏移
    send_time: float       # 发送时间
    retry_count: int = 0   # 重传次数
    acked: bool = False    # 是否已确认


@dataclass
class RTTStats:
    """RTT统计信息"""
    rtts: List[float] = field(default_factory=list)
    #定义了一个存储浮点数的列表用于存放rtt样本，初始默认值为空
    #不用[]，因为[]会使得所有实例共享同一个列表，而不是每个实例都有自己的列表
    
    def add_rtt(self, rtt: float):
        """添加RTT样本"""
        self.rtts.append(rtt)
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        if not self.rtts:
            return {
                'max_rtt': 0,
                'min_rtt': 0,
                'avg_rtt': 0,
                'std_rtt': 0
            }
        
        df = pd.DataFrame({'rtt': self.rtts})
        return {
            'max_rtt': df['rtt'].max(),
            'min_rtt': df['rtt'].min(),
            'avg_rtt': df['rtt'].mean(),
            'std_rtt': df['rtt'].std()
        }
        # 传入rtts这个列表，返回一个字典，包含最大RTT、最小RTT、平均RTT和标准差RTT


class UDPClient:
    """UDP客户端"""
    
    def __init__(self, server_ip: str, server_port: int, student_id_last4: int,
                 timeout: float = DEFAULT_TIMEOUT, total_packets: int = TOTAL_PACKETS_TO_SEND,
                 window_size: int = WINDOW_SIZE):
        self.server_ip = server_ip
        self.server_port = server_port
        self.server_address = (server_ip, server_port)
        self.student_id_last4 = student_id_last4
        self.timeout = timeout
        self.total_packets = total_packets
        self.window_size = window_size  # 窗口大小（字节）
        
        # 根据窗口大小计算窗口最大包数
        self.max_packets_in_window = min(window_size // MIN_DATA_SIZE, DEFAULT_MAX_PACKETS_IN_WINDOW)
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.logger = Logger("run_log.txt")
        
        # 连接状态
        self.connected = False
        self.client_seq = 0  # 客户端序列号
        
        # 发送窗口
        self.window: List[PacketInfo] = []  # 发送窗口
        self.next_seq = 1  # 下一个要发送的序列号
        self.base_seq = 1  # 窗口基序号（最早的未确认包）
        self.next_data_offset = 0  # 下一个要发送的数据偏移
        
        # 数据包跟踪
        self.packet_num = 0  # 已发送的数据包编号（用于打印）
        self.packets_to_send: List[bytes] = []  # 待发送的数据
        
        # 统计信息
        self.rtt_stats = RTTStats()
        self.total_sent = 0  # 总发送次数（包括重传）
        self.retransmissions = 0  # 重传次数
        self.successfully_acked = 0  # 成功确认的包数
        
        # 动态超时
        self.estimated_rtt = timeout
        self.dev_rtt = 0
        
        self.logger.log_info(f"客户端初始化完成")
        self.logger.log_info(f"目标服务器: {server_ip}:{server_port}")
        self.logger.log_info(f"初始超时时间: {timeout*1000:.0f}ms")
        self.logger.log_info(f"计划发送数据包数: {total_packets}")
        self.logger.log_info(f"发送窗口大小: {window_size}字节 (最多{self.max_packets_in_window}个包)")
        
        print(f"\n{'='*60}")
        print(f"UDP客户端已启动")
        print(f"目标服务器: {server_ip}:{server_port}")
        print(f"初始超时时间: {timeout*1000:.0f}ms")
        print(f"计划发送数据包数: {total_packets}")
        print(f"发送窗口大小: {window_size}字节 (最多{self.max_packets_in_window}个包)")
        print(f"{'='*60}\n")
    
    def generate_data(self):
        """生成要发送的数据"""
        # 生成随机数据
        total_data_size = self.total_packets * MAX_DATA_SIZE
        data = bytes([random.randint(0, 255) for _ in range(total_data_size)])
        
        # 切分成数据包
        offset = 0
        packet_idx = 0
        while offset < len(data) and packet_idx < self.total_packets:
            # 随机决定数据包大小（40-80字节）
            packet_size = random.randint(MIN_DATA_SIZE, MAX_DATA_SIZE)
            # 或者固定80字节
            packet_size = MAX_DATA_SIZE
            
            end_offset = min(offset + packet_size, len(data))
            packet_data = data[offset:end_offset]
            self.packets_to_send.append(packet_data)
            
            offset = end_offset
            packet_idx += 1
        
        self.logger.log_info(f"生成了 {len(self.packets_to_send)} 个数据包")
    
    def connect(self) -> bool: #客户端的connect需要返回值，服务器的connect不需要返回值
                               # 返回值为bool，表示连接是否成功
        """建立连接（三次握手）"""
        self.logger.separator("连接建立阶段")
        print("[连接] 开始三次握手...")
        
        # 1. 发送SYN
        syn_packet = create_syn_packet(self.student_id_last4)
        self.socket.sendto(syn_packet.pack(), self.server_address)
        self.logger.log_send("SYN", seq_num=0)
        print(f"[连接] 发送SYN报文")
        
        # 2. 接收SYN-ACK
        try:
            data, addr = self.socket.recvfrom(1024)
            packet = Packet.unpack(data)
            
            if packet.header.packet_type != PacketType.SYN_ACK:
                self.logger.log_error(f"期望SYN-ACK，收到: {packet.header.packet_type}")
                return False
            
            self.logger.log_recv("SYN-ACK", seq_num=packet.header.seq_num, ack_num=packet.header.ack_num)
            print(f"[连接] 收到SYN-ACK报文")
            
        except socket.timeout:
            self.logger.log_error("等待SYN-ACK超时")
            print(f"[连接错误] 等待SYN-ACK超时")
            return False
        
        # 3. 发送ACK
        ack_packet = create_ack_packet(1, packet.header.seq_num + 1, self.student_id_last4)
        self.socket.sendto(ack_packet.pack(), self.server_address)
        self.logger.log_send("ACK", seq_num=1, ack_num=packet.header.seq_num + 1)
        print(f"[连接] 发送ACK报文，三次握手完成\n")
        
        self.connected = True
        self.client_seq = 1
        self.logger.log_connection("三次握手完成", f"已连接到 {self.server_address}")
        return True
    
    def calculate_timeout(self, sample_rtt: float): #RTT全称：Round Trip Time
        """
        根据RTT样本计算超时时间
        使用简化的TCP超时计算公式
        """
        alpha = 0.125
        beta = 0.25
        
        #estimated_rtt：在UDPclient类初始化时设置为timeout
        self.estimated_rtt = (1 - alpha) * self.estimated_rtt + alpha * sample_rtt
        self.dev_rtt = (1 - beta) * self.dev_rtt + beta * abs(sample_rtt - self.estimated_rtt)
        
        # 超时时间 = EstimatedRTT + 4 * DevRTT
        self.timeout = max(self.estimated_rtt + 4 * self.dev_rtt, 0.1)
        self.timeout = min(self.timeout, 2.0)  # 最大2秒
        
        self.socket.settimeout(self.timeout)
        self.logger.log_info(f"更新超时时间: {self.timeout*1000:.0f}ms (RTT样本: {sample_rtt*1000:.0f}ms)")
    
    def send_packet(self, packet_info: PacketInfo):
        """发送单个数据包"""
        packet = create_data_packet(
            packet_info.seq_num,
            packet_info.data,
            self.student_id_last4
        )
        #sendto方法参数的含义：sendto(data, (host, port))
        # data：要发送的数据，在这里是packet.pack()，bytes类型的数据
        # (host, port)：要发送到的服务器地址和端口
        self.socket.sendto(packet.pack(), self.server_address)
        packet_info.send_time = time.time()
        
        self.total_sent += 1
        self.logger.log_send(
            "DATA",
            seq_num=packet_info.seq_num,
            data_range=(packet_info.data_start, packet_info.data_end),
            data_len=len(packet_info.data)
        )
        print(f"第{self.packet_num}个（第{packet_info.data_start}~{packet_info.data_end}字节）client端已经发送")
    
    def fill_window(self):
        """填充发送窗口"""
        #窗口未满且还有数据要发送
        while (len(self.window) < self.window_size // MIN_DATA_SIZE and 
               len(self.window) < self.max_packets_in_window and
               self.next_seq <= self.total_packets):
            
            if self.next_seq - 1 >= len(self.packets_to_send):
                break
            
            data = self.packets_to_send[self.next_seq - 1]
            data_start = self.next_data_offset
            data_end = self.next_data_offset + len(data) - 1
            
            #创建新的PacketInfo对象
            packet_info = PacketInfo(
                seq_num=self.next_seq,
                data=data,
                data_start=data_start,
                data_end=data_end,
                send_time=0.0
            )
            
            #将新的PacketInfo对象添加到窗口window中
            self.window.append(packet_info)
            self.next_seq += 1
            self.next_data_offset = data_end + 1
    
    def send_window(self):
        """发送窗口中的所有未确认包"""
        #遍历窗口window，发送所有未确认的包
        #send_packet方法会更新send_time属性
        for packet_info in self.window:
            if not packet_info.acked and packet_info.send_time == 0:
                self.packet_num += 1
                self.send_packet(packet_info)
    
    def handle_ack(self, ack_num: int, server_time: str):
        """处理累积确认"""
        # 计算RTT
        current_time = time.time()
        for packet_info in self.window:
            #找到确认的包，即seq_num等于ack_num-1的包
            if packet_info.seq_num == ack_num - 1 and packet_info.send_time > 0:
                rtt = (current_time - packet_info.send_time) * 1000  # 转换为毫秒
                self.rtt_stats.add_rtt(rtt)
                self.calculate_timeout(rtt / 1000)  # 转换回秒
                break
        
        # 累积确认：确认所有序号小于ack_num的包
        acked_count = 0
        for packet_info in self.window[:]:
            if packet_info.seq_num < ack_num and not packet_info.acked:
                packet_info.acked = True #把所有序号小于ack_num的包都标记为已确认
                self.successfully_acked += 1
                acked_count += 1
                
                self.logger.log_recv(
                    "DATA-ACK",
                    ack_num=ack_num,
                    extra=f"RTT={self.rtt_stats.rtts[-1]:.2f}ms, 服务器时间={server_time}"
                )
                print(f"第{self.packet_num - len(self.window) + self.window.index(packet_info) + 1}个"
                      f"（第{packet_info.data_start}~{packet_info.data_end}字节）"
                      f"server端已经收到，RTT是{self.rtt_stats.rtts[-1]:.2f}ms")
        
        # 移除已确认的包，滑动窗口
        while self.window and self.window[0].acked:
            self.window.pop(0) #移除窗口window中的第一个包
            self.base_seq += 1 #更新base_seq窗口基序号，指向下一个未确认的包
    
    def handle_timeout(self):
        """处理超时，重传窗口中的所有未确认包"""
        current_time = time.time()
        
        for packet_info in self.window:
            if not packet_info.acked and packet_info.send_time > 0:
                if current_time - packet_info.send_time > self.timeout:
                    # 超时，需要重传
                    self.retransmissions += 1
                    packet_info.retry_count += 1
                    
                    self.logger.log_timeout(
                        packet_info.seq_num,
                        (packet_info.data_start, packet_info.data_end)
                    )
                    
                    # 重传
                    self.packet_num += 1
                    self.send_packet(packet_info)
                    self.logger.log_retransmit(
                        packet_info.seq_num,
                        (packet_info.data_start, packet_info.data_end),
                        packet_info.retry_count
                    )
                    print(f"重传第{packet_info.seq_num}个"
                          f"（第{packet_info.data_start}~{packet_info.data_end}字节）数据包")
    
    def data_transfer(self):
        """数据传输阶段"""
        self.logger.separator("数据传输阶段")
        print(f"\n{'='*60}")
        print("开始数据传输...")
        print(f"{'='*60}\n")
        
        # 生成数据
        self.generate_data()
        
        # 主循环
        while self.successfully_acked < self.total_packets:
            # 填充窗口
            self.fill_window()
            
            # 发送窗口中的包
            self.send_window()
            
            # 等待ACK
            try:
                data, addr = self.socket.recvfrom(1024)
                packet = Packet.unpack(data)
                
                if packet.header.packet_type == PacketType.DATA_ACK:
                    server_time = packet.data.decode() if packet.data else ""
                    self.handle_ack(packet.header.ack_num, server_time)
                    
            except socket.timeout:
                # 超时，检查是否需要重传
                self.handle_timeout()
        
        self.logger.log_info(f"数据传输完成，共发送 {self.total_packets} 个数据包")
        print(f"\n[传输完成] 所有数据包已成功发送并确认")
    
    def disconnect(self):
        """断开连接"""
        self.logger.separator("连接释放阶段")
        print(f"\n[连接] 开始断开连接...")
        
        # 发送FIN
        fin_packet = create_fin_packet(self.client_seq, self.student_id_last4)
        self.socket.sendto(fin_packet.pack(), self.server_address)
        self.logger.log_send("FIN", seq_num=self.client_seq)
        print(f"[连接] 发送FIN报文")
        
        # 接收FIN-ACK
        try:
            data, addr = self.socket.recvfrom(1024)
            packet = Packet.unpack(data)
            
            if packet.header.packet_type == PacketType.FIN_ACK:
                self.logger.log_recv("FIN-ACK", seq_num=packet.header.seq_num, ack_num=packet.header.ack_num)
                print(f"[连接] 收到FIN-ACK报文，连接已关闭")
            else:
                self.logger.log_error(f"期望FIN-ACK，收到: {packet.header.packet_type}")
                
        except socket.timeout:
            self.logger.log_error("等待FIN-ACK超时")
            print(f"[连接警告] 等待FIN-ACK超时")
        
        self.logger.log_connection("连接已关闭")
    
    def print_summary(self):
        """打印汇总信息"""
        stats = self.rtt_stats.get_stats()
        
        # 计算丢包率
        loss_rate = self.retransmissions / self.total_sent if self.total_sent > 0 else 0
        
        summary = {
            'total_packets': self.total_packets,
            'actual_sent': self.total_sent,
            'retransmissions': self.retransmissions,
            'loss_rate': loss_rate,
            'max_rtt': stats['max_rtt'],
            'min_rtt': stats['min_rtt'],
            'avg_rtt': stats['avg_rtt'],
            'std_rtt': stats['std_rtt']
        }
        
        self.logger.log_summary(summary)
        
        print(f"\n{'='*60}")
        print("【汇总】")
        print(f"丢包率: {loss_rate:.2%}")
        print(f"最大RTT: {stats['max_rtt']:.2f}ms")
        print(f"最小RTT: {stats['min_rtt']:.2f}ms")
        print(f"平均RTT: {stats['avg_rtt']:.2f}ms")
        print(f"RTT标准差: {stats['std_rtt']:.2f}ms")
        print(f"{'='*60}\n")
    
    def run(self):
        """运行客户端"""
        try:
            # 三次握手
            if not self.connect():
                self.logger.log_error("连接建立失败")
                return
            
            # 数据传输
            self.data_transfer()
            
            # 断开连接
            self.disconnect()
            
            # 打印汇总
            self.print_summary()
            
        except KeyboardInterrupt:
            self.logger.log_info("客户端被用户中断")
            print("\n[客户端] 被用户中断")
        except Exception as e:
            self.logger.log_error(f"客户端错误: {str(e)}")
            print(f"\n[错误] {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.socket.close()
            self.logger.log_info("客户端关闭")


def main():
    parser = argparse.ArgumentParser(description='UDP可靠传输客户端')
    parser.add_argument('-i', '--ip', type=str, required=True, help='服务器IP地址')
    parser.add_argument('-p', '--port', type=int, required=True, help='服务器端口')
    parser.add_argument('-s', '--student-id', type=int, required=True, 
                        help='学号后4位数字')
    parser.add_argument('-t', '--timeout', type=float, default=DEFAULT_TIMEOUT,
                        help=f'初始超时时间(秒)，默认{DEFAULT_TIMEOUT}')
    parser.add_argument('-n', '--num-packets', type=int, default=TOTAL_PACKETS_TO_SEND,
                        help=f'发送数据包数量，默认{TOTAL_PACKETS_TO_SEND}')
    parser.add_argument('-w', '--window-size', type=int, default=WINDOW_SIZE,
                        help=f'发送窗口大小(字节)，默认{WINDOW_SIZE}')
    
    args = parser.parse_args()
    
    if not (0 <= args.student_id <= 9999):
        print("错误: 学号后4位必须是0-9999之间的数字")
        return
    
    if args.timeout <= 0:
        print("错误: 超时时间必须大于0")
        return
    
    if args.window_size < MIN_DATA_SIZE:
        print(f"错误: 窗口大小必须至少为{MIN_DATA_SIZE}字节")
        return
    
    client = UDPClient(
        args.ip, 
        args.port, 
        args.student_id,
        args.timeout,
        args.num_packets,
        args.window_size
    )
    client.run()


if __name__ == '__main__':
    main()