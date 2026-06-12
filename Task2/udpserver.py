"""
UDP可靠传输 - 服务器端
模拟TCP连接建立和可靠数据传输
"""

import socket
import random
import argparse
from typing import Dict, Set
from datetime import datetime

from protocol import (
    Packet, PacketType, PacketHeader,
    HEADER_SIZE, PACKET_LOSS_RATE,
    verify_student_id, create_syn_ack_packet,
    create_ack_packet, create_data_ack_packet,
    create_fin_ack_packet, get_current_time_str
)
from logger import Logger


class UDPServer:
    """UDP服务器"""
    
    def __init__(self, port: int, student_id_last4: int, loss_rate: float = PACKET_LOSS_RATE):
        self.port = port
        self.student_id_last4 = student_id_last4
        self.loss_rate = loss_rate
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
         #SOCK_DGRAM表示UDP套接字，TCP是SOCK_STREAM
        self.socket.bind(('0.0.0.0', port))  # 绑定到所有IP地址，监听指定端口
        self.logger = Logger("run_log_server.txt")
        
        # 连接状态
        self.connected = False
        self.client_address = None
        self.server_seq = random.randint(0, 1000)  # 服务器初始序列号
        
        # 数据接收状态
        self.received_packets: Dict[int, bytes] = {}  # seq_num -> data
        # 用于存储已接收的报文，键为序列号，值为数据；用于缓存乱序到达的数据包
        self.expected_seq = 1  # 期望接收的下一个序列号
        self.total_received = 0
        
        # 统计信息
        self.total_packets_received = 0
        self.total_packets_dropped = 0
        
        self.logger.log_info(f"服务器启动，监听端口: {port}")
        self.logger.log_info(f"丢包率设置: {loss_rate*100:.1f}%")
        print(f"\n{'='*60}")
        print(f"UDP服务器已启动")
        print(f"监听端口: {port}")
        print(f"丢包率: {loss_rate*100:.1f}%")
        print(f"{'='*60}\n")
    
    def handle_connection(self):
        """处理连接建立（三次握手）"""
        # 1. 接收SYN
        self.logger.log_connection("等待连接请求(SYN)...")
        data, addr = self.socket.recvfrom(1024) #UDP的recvfrom方法会阻塞等待接收数据，直到有数据到达
                                                #UDP不能像TCP那样使用accept方法，因为UDP是无连接的，每个数据包都是独立的
                                                #一次必须接收完整的一个数据包。
                                                #1024在这里是指接收缓冲区的大小，超过1024字节的数据会被截断，不足1024字节的数据会直接被接受
        packet = Packet.unpack(data)
        
        if packet.header.packet_type != PacketType.SYN:
            self.logger.log_error(f"期望SYN报文，收到类型: {packet.header.packet_type}")
            return False
        
        self.logger.log_recv("SYN", seq_num=packet.header.seq_num)
        
        # 验证StudentID
        is_valid, student_id = verify_student_id(packet.header.student_id)
        if not is_valid:
            self.logger.log_error(f"StudentID验证失败! 收到的值: {packet.header.student_id}, 解密后: {student_id}")
            print(f"\n[错误] StudentID验证失败! 解密后的值: {student_id} (应为0-9999范围)")
            return False
        
        # 验证StudentID是否与服务器设置的一致
        if student_id != self.student_id_last4:
            self.logger.log_error(f"StudentID不匹配! 客户端: {student_id}, 服务器: {self.student_id_last4}")
            print(f"\n[错误] StudentID不匹配! 客户端学号后4位: {student_id}, 服务器学号后4位: {self.student_id_last4}")
            return False
        
        self.logger.log_connection("SYN接收成功", f"StudentID验证通过: {student_id}")
        print(f"[连接] 收到来自 {addr} 的连接请求，StudentID验证通过")
        
        # 2. 发送SYN-ACK
        syn_ack = create_syn_ack_packet(self.student_id_last4, self.server_seq)
        self.socket.sendto(syn_ack.pack(), addr)
        self.logger.log_send("SYN-ACK", seq_num=self.server_seq, extra="确认客户端SYN")
        
        # 3. 接收ACK
        data, addr = self.socket.recvfrom(1024) 
        packet = Packet.unpack(data)
        
        if packet.header.packet_type != PacketType.ACK:
            self.logger.log_error(f"期望ACK报文，收到类型: {packet.header.packet_type}")
            return False
        
        self.logger.log_recv("ACK", ack_num=packet.header.ack_num)
        self.logger.log_connection("三次握手完成", f"客户端 {addr} 已连接")
        print(f"[连接] 三次握手完成，客户端 {addr} 已连接\n")
        
        self.connected = True
        self.client_address = addr
        return True
    
    def should_drop_packet(self) -> bool:
        """随机决定是否丢包"""
        return random.random() < self.loss_rate
    
    def handle_data_transfer(self):
        """处理数据传输阶段"""
        self.logger.separator("数据传输阶段开始")
        print(f"\n{'='*60}")
        print("开始数据传输...")
        print(f"{'='*60}\n")
        
        last_ack_sent = 0  # 最后发送的累积确认号
        
        while True:
            try:
                self.socket.settimeout(30.0)  # 设置超时，防止无限阻塞等待数据，属于保护机制
                data, addr = self.socket.recvfrom(2048) #DATA报文会比FIN报文大，所以这里的缓冲区大小变成了2048字节
                
                if addr != self.client_address:
                    self.logger.log_error(f"收到未知地址的数据: {addr}")
                    continue
                
                packet = Packet.unpack(data)
                self.total_packets_received += 1
                
                # 检查是否是FIN报文
                if packet.header.packet_type == PacketType.FIN:
                    self.handle_fin(packet)
                    break
                
                if packet.header.packet_type != PacketType.DATA:
                    self.logger.log_error(f"期望DATA报文，收到类型: {packet.header.packet_type}")
                    continue
                
                seq_num = packet.header.seq_num
                data_len = packet.header.data_len
                
                self.logger.log_recv("DATA", seq_num=seq_num, data_len=data_len)
                
                # 模拟丢包
                if self.should_drop_packet():
                    self.total_packets_dropped += 1
                    self.logger.log_packet_loss(seq_num, f"随机丢包 (丢包率: {self.loss_rate*100:.1f}%)")
                    print(f"[丢包] seq={seq_num} 数据包被丢弃")
                    continue  # 不发送ACK，模拟丢包
                
                # 接收数据包
                if seq_num == self.expected_seq:
                    # 按序到达
                    self.received_packets[seq_num] = packet.data
                    self.expected_seq += 1
                    
                    # 检查是否有后续缓存的包
                    while self.expected_seq in self.received_packets:
                        self.expected_seq += 1
                    
                    last_ack_sent = self.expected_seq
                    self.total_received += 1
                    
                elif seq_num > self.expected_seq:
                    # 乱序到达，缓存起来
                    if seq_num not in self.received_packets:
                        self.received_packets[seq_num] = packet.data
                        self.logger.log_info(f"缓存乱序包 seq={seq_num}")
                else:
                    # seq_num < expected_seq，重复包
                    self.logger.log_info(f"收到重复包 seq={seq_num}")
                
                # 发送累积确认
                server_time = get_current_time_str()
                ack_packet = create_data_ack_packet(last_ack_sent, self.student_id_last4, server_time)
                self.socket.sendto(ack_packet.pack(), addr)
                self.logger.log_send("DATA-ACK", seq_num=0, extra=f"累积确认号={last_ack_sent}, 服务器时间={server_time}")
                print(f"[确认] 发送累积确认 ack={last_ack_sent}, 已接收 {self.total_received} 个数据包")
                
            except socket.timeout:
                self.logger.log_error("数据传输超时，等待客户端...")
                continue
            except Exception as e:
                self.logger.log_error(f"数据传输错误: {str(e)}")
                break
    
    def handle_fin(self, packet: Packet):
        """处理连接释放"""
        self.logger.log_recv("FIN", seq_num=packet.header.seq_num)
        print(f"\n[连接] 收到FIN报文，准备关闭连接")
        
        # 发送FIN-ACK
        fin_ack = create_fin_ack_packet(self.server_seq + 1, packet.header.seq_num + 1, self.student_id_last4)
        self.socket.sendto(fin_ack.pack(), self.client_address)
        self.logger.log_send("FIN-ACK", seq_num=self.server_seq + 1, ack_num=packet.header.seq_num + 1)
        
        self.logger.log_connection("连接关闭")
        print(f"[连接] 连接已关闭")
        
        # 打印统计信息
        self.logger.separator("服务器统计")
        print(f"\n{'='*60}")
        print(f"【服务器统计】")
        print(f"总接收数据包: {self.total_packets_received}")
        print(f"丢弃数据包: {self.total_packets_dropped}")
        print(f"成功接收数据包: {self.total_received}")
        if self.total_packets_received > 0:
            actual_loss_rate = self.total_packets_dropped / self.total_packets_received
            print(f"实际丢包率: {actual_loss_rate*100:.2f}%")
        print(f"{'='*60}\n")
    
    def run(self):
        """运行服务器"""
        try:
            # 三次握手
            if not self.handle_connection():
                self.logger.log_error("连接建立失败")
                return
            
            # 数据传输
            self.handle_data_transfer()
            
        except KeyboardInterrupt:
            self.logger.log_info("服务器被用户中断")
            print("\n[服务器] 被用户中断")
        except Exception as e:
            self.logger.log_error(f"服务器错误: {str(e)}")
            print(f"\n[错误] {str(e)}")
        finally:
            self.socket.close()
            self.logger.log_info("服务器关闭")


def main():
    parser = argparse.ArgumentParser(description='UDP可靠传输服务器')
    parser.add_argument('-p', '--port', type=int, required=True, help='服务器监听端口')
    parser.add_argument('-s', '--student-id', type=int, required=True, 
                        help='学号后4位数字')
    parser.add_argument('-l', '--loss-rate', type=float, default=0.15,
                        help='丢包率 (默认: 0.15)')
    
    args = parser.parse_args()
    
    if not (0 <= args.student_id <= 9999):
        print("错误: 学号后4位必须是0-9999之间的数字")
        return
    
    if not (0 <= args.loss_rate <= 1):
        print("错误: 丢包率必须在0-1之间")
        return
    
    server = UDPServer(args.port, args.student_id, args.loss_rate)
    server.run()


if __name__ == '__main__':
    main()