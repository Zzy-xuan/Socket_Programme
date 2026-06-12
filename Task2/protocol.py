"""
自定义UDP应用层协议定义
模拟TCP的连接建立和可靠数据传输
"""

import struct  # 用于将数据打包/解包为二进制字节流
from enum import IntEnum  # 用于创建整数枚举类型
from dataclasses import dataclass  # 用于自动生成类的__init__等方法
from datetime import datetime  # 用于获取当前时间

# 常量定义
XOR_KEY = 0x5A3C  # StudentID的XOR密钥
DEFAULT_TIMEOUT = 0.3  # 默认超时时间300ms
MIN_DATA_SIZE = 40  # 每个UDP数据包最小数据大小
MAX_DATA_SIZE = 80  # 每个UDP数据包最大数据大小
WINDOW_SIZE = 400  # 发送窗口大小（字节）
MAX_PACKETS_IN_WINDOW = 10  # 窗口内最大数据包数
MIN_PACKETS_IN_WINDOW = 5  # 窗口内最小数据包数
TOTAL_PACKETS_TO_SEND = 30  # 总共要发送的数据包数
PACKET_LOSS_RATE = 0.15  # 丢包率15%


class PacketType(IntEnum):
    """报文类型"""
    SYN = 1        # 连接请求
    SYN_ACK = 2    # 连接请求确认
    ACK = 3        # 确认
    DATA = 4       # 数据报文
    DATA_ACK = 5   # 数据确认
    FIN = 6        # 连接释放请求
    FIN_ACK = 7    # 连接释放确认


# 报文首部格式
"""
首部格式（共11字节）:
| 类型(1B) | 序列号(2B) | 确认号(2B) | 数据长度(2B) | StudentID(2B) | 时间戳(2B) |
"""
HEADER_FORMAT = '!BHHHHH'  # !表示网络字节序(大端)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 11字节


@dataclass  # 装饰器，自动生成__init__、__repr__等方法，简化类定义
class PacketHeader:
    """报文首部"""
    packet_type: int      # 报文类型 (1字节)
    seq_num: int          # 序列号 (2字节, 无符号短整型)
    ack_num: int          # 确认号 (2字节, 无符号短整型)
    data_len: int         # 数据长度 (2字节, 无符号短整型)
    student_id: int       # 学生ID (2字节, 无符号短整型)
    timestamp: int = 0    # 时间戳 (2字节, 用于RTT计算)

    def pack(self) -> bytes:  # -> bytes表示返回值类型为bytes字节流
        """将首部打包为字节流"""
        return struct.pack(
            HEADER_FORMAT,
            self.packet_type,
            self.seq_num,
            self.ack_num,
            self.data_len,
            self.student_id,
            self.timestamp
        )

    @staticmethod  # 装饰器，表示静态方法，不需要实例即可调用，通过类名直接调用
    def unpack(data: bytes) -> 'PacketHeader':  # -> 'PacketHeader'表示返回值类型为PacketHeader对象
        """从字节流解包首部"""
        unpacked = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        return PacketHeader(
            packet_type=unpacked[0],
            seq_num=unpacked[1],
            ack_num=unpacked[2],
            data_len=unpacked[3],
            student_id=unpacked[4],
            timestamp=unpacked[5]
        )


class Packet:
    """完整报文（首部+数据）"""
    
    def __init__(self, header: PacketHeader, data: bytes = b''):
        self.header = header
        self.data = data
    
    def pack(self) -> bytes:  # -> bytes表示返回值类型为bytes字节流
        """打包完整报文"""
        return self.header.pack() + self.data
    
    @staticmethod  # 装饰器，表示静态方法，不需要实例即可调用
    def unpack(data: bytes) -> 'Packet':  # -> 'Packet'表示返回值类型为Packet对象
        """解包完整报文"""
        header = PacketHeader.unpack(data)
        packet_data = data[HEADER_SIZE:HEADER_SIZE + header.data_len]
        return Packet(header, packet_data)
    
    def __repr__(self):  # 用于将类对象转换为字符串打印
        type_name = PacketType(self.header.packet_type).name if self.header.packet_type in range(1, 8) else "UNKNOWN"
        return f"Packet(type={type_name}, seq={self.header.seq_num}, ack={self.header.ack_num}, len={self.header.data_len})"


# 工具函数

def calculate_student_id(last_four_digits: int) -> int:
    """
    计算StudentID字段值
    取学号后4位与0x5A3C做XOR运算
    """
    return last_four_digits ^ XOR_KEY


def verify_student_id(student_id: int) -> tuple[bool, int]:
    """
    验证StudentID字段是否合法
    返回: (是否合法, 解密后的学号后4位)
    """
    decrypted = student_id ^ XOR_KEY
    is_valid = 0 <= decrypted <= 9999
    return is_valid, decrypted


def get_current_time_ms() -> int:
    """获取当前时间的毫秒部分（用于RTT计算）"""
    now = datetime.now()
    return now.microsecond // 1000


def get_current_time_str() -> str:
    """获取当前时间字符串 hh-mm-ss"""
    return datetime.now().strftime("%H-%M-%S")


def create_syn_packet(student_id_last4: int) -> Packet:
    """创建SYN连接请求报文"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.SYN,
        seq_num=0,
        ack_num=0,
        data_len=0,
        student_id=student_id_field
    )
    return Packet(header)


def create_syn_ack_packet(student_id_last4: int, server_seq: int) -> Packet:
    """创建SYN-ACK响应报文"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.SYN_ACK,
        seq_num=server_seq,
        ack_num=1,  # 确认客户端的SYN
        data_len=0,
        student_id=student_id_field
    )
    return Packet(header)


def create_ack_packet(seq_num: int, ack_num: int, student_id_last4: int) -> Packet:
    """创建ACK确认报文"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.ACK,
        seq_num=seq_num,
        ack_num=ack_num,
        data_len=0,
        student_id=student_id_field
    )
    return Packet(header)


def create_data_packet(seq_num: int, data: bytes, student_id_last4: int) -> Packet:
    """创建数据报文"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.DATA,
        seq_num=seq_num,
        ack_num=0,
        data_len=len(data),
        student_id=student_id_field,
        timestamp=get_current_time_ms()
    )
    return Packet(header, data)


def create_data_ack_packet(ack_num: int, student_id_last4: int, server_time: str) -> Packet:
    """创建数据确认报文（累积确认）"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.DATA_ACK,
        seq_num=0,
        ack_num=ack_num,  # 累积确认号
        data_len=0,
        student_id=student_id_field
    )
    return Packet(header, server_time.encode()) #server_time会作为data字段传入
                                                #但此时data_len为0，首部不会记录server_time的长度


def create_fin_packet(seq_num: int, student_id_last4: int) -> Packet:
    """创建FIN连接释放报文"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.FIN,
        seq_num=seq_num,
        ack_num=0,
        data_len=0,
        student_id=student_id_field
    )
    return Packet(header)


def create_fin_ack_packet(seq_num: int, ack_num: int, student_id_last4: int) -> Packet:
    """创建FIN-ACK响应报文"""
    student_id_field = calculate_student_id(student_id_last4)
    header = PacketHeader(
        packet_type=PacketType.FIN_ACK,
        seq_num=seq_num,
        ack_num=ack_num,
        data_len=0,
        student_id=student_id_field
    )
    return Packet(header)