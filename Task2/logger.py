"""
运行日志工具
记录每一次发包、收包、超时、重传事件
"""

import os
from datetime import datetime
from typing import Optional


class Logger:
    """日志记录器"""
    
    def __init__(self, log_file: str = "run_log.txt"):
        self.log_file = log_file
        # 清空或创建日志文件
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write(f"{'='*60}\n")
            f.write(f"UDP可靠传输实验运行日志\n")
            f.write(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}\n")
            f.write(f"{'='*60}\n\n")
    
    def _get_timestamp(self) -> str:
        """获取精确时间戳"""
        return datetime.now().strftime('%H:%M:%S.%f')[:-3]
    
    def _write(self, message: str):
        """写入日志"""
        timestamp = self._get_timestamp()
        log_entry = f"[{timestamp}] {message}\n"
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
        # 同时打印到控制台
        print(log_entry.strip())
    
    def log_send(self, packet_type: str, seq_num: int = 0, ack_num: int = None, 
                 data_range: tuple = None, data_len: int = 0, extra: str = ""):
        """
        记录发送事件
        
        Args:
            packet_type: 报文类型 (SYN/DATA/FIN等)
            seq_num: 序列号
            ack_num: 确认号
            data_range: 数据字节范围 (start, end)
            data_len: 数据长度
            extra: 额外信息
        """
        parts = [f"发送 [{packet_type}]"]
        if seq_num is not None and seq_num > 0:
            parts.append(f"seq={seq_num}")
        if ack_num is not None:
            parts.append(f"ack={ack_num}")
        if data_range:
            parts.append(f"数据范围: {data_range[0]}~{data_range[1]}字节")
            parts.append(f"长度={data_len}字节")
        elif data_len > 0:
            parts.append(f"长度={data_len}字节")
        if extra:
            parts.append(extra)
        
        msg = ", ".join(parts)
        self._write(msg)
    
    def log_recv(self, packet_type: str, seq_num: int = None, ack_num: int = None,
                 data_len: int = 0, server_time: str = None, extra: str = ""):
        """
        记录接收事件
        
        Args:
            packet_type: 报文类型
            seq_num: 序列号
            ack_num: 确认号
            data_len: 数据长度
            server_time: 服务器时间
            extra: 额外信息
        """
        parts = [f"接收 [{packet_type}]"]
        if seq_num is not None:
            parts.append(f"seq={seq_num}")
        if ack_num is not None:
            parts.append(f"ack={ack_num}")
        if data_len > 0:
            parts.append(f"长度={data_len}字节")
        if server_time:
            parts.append(f"服务器时间={server_time}")
        if extra:
            parts.append(extra)
        
        msg = ", ".join(parts)
        self._write(msg)
    
    def log_timeout(self, seq_num: int, data_range: tuple = None):
        """
        记录超时事件
        
        Args:
            seq_num: 序列号
            data_range: 数据字节范围
        """
        if data_range:
            msg = f"超时 seq={seq_num}, 数据范围: {data_range[0]}~{data_range[1]}字节"
        else:
            msg = f"超时 seq={seq_num}"
        self._write(msg)
    
    def log_retransmit(self, packet_num: int, data_range: tuple, retry_count: int = 1):
        """
        记录重传事件
        
        Args:
            packet_num: 数据包编号
            data_range: 数据字节范围
            retry_count: 重传次数
        """
        msg = f"重传 第{packet_num}个 (第{data_range[0]}~{data_range[1]}字节) 数据包, 第{retry_count}次重传"
        self._write(msg)
    
    def log_connection(self, event: str, details: str = ""):
        """
        记录连接事件
        
        Args:
            event: 事件描述
            details: 详细信息
        """
        msg = f"连接事件: {event}"
        if details:
            msg += f" - {details}"
        self._write(msg)
    
    def log_error(self, error_msg: str):
        """
        记录错误事件
        
        Args:
            error_msg: 错误信息
        """
        self._write(f"错误: {error_msg}")
    
    def log_info(self, info_msg: str):
        """
        记录一般信息
        
        Args:
            info_msg: 信息内容
        """
        self._write(f"信息: {info_msg}")
    
    def log_packet_loss(self, seq_num: int, reason: str = "模拟丢包"):
        """
        记录丢包事件（服务器端）
        
        Args:
            seq_num: 序列号
            reason: 丢包原因
        """
        self._write(f"丢包: seq={seq_num}, 原因: {reason}")
    
    def log_summary(self, stats: dict):
        """
        记录汇总信息
        
        Args:
            stats: 统计信息字典
        """
        self._write("\n" + "="*60)
        self._write("【汇总信息】")
        self._write("="*60)
        
        if 'total_packets' in stats:
            self._write(f"计划发送数据包数: {stats['total_packets']}")
        if 'actual_sent' in stats:
            self._write(f"实际发送UDP包数: {stats['actual_sent']}")
        if 'retransmissions' in stats:
            self._write(f"重传次数: {stats['retransmissions']}")
        if 'loss_rate' in stats:
            self._write(f"丢包率: {stats['loss_rate']:.2%}")
        if 'max_rtt' in stats:
            self._write(f"最大RTT: {stats['max_rtt']:.2f}ms")
        if 'min_rtt' in stats:
            self._write(f"最小RTT: {stats['min_rtt']:.2f}ms")
        if 'avg_rtt' in stats:
            self._write(f"平均RTT: {stats['avg_rtt']:.2f}ms")
        if 'std_rtt' in stats:
            self._write(f"RTT标准差: {stats['std_rtt']:.2f}ms")
        
        self._write("="*60)
        self._write(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
        self._write("="*60 + "\n")
    
    def separator(self, title: str = ""):
        """添加分隔线"""
        if title:
            self._write(f"\n{'='*20} {title} {'='*20}\n")
        else:
            self._write("-" * 60)