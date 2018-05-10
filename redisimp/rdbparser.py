# Borrowed from rdb-tools
import sys
import struct
from .crc64 import crc64

__all__ = ['parse_rdb']


REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 0x80
REDIS_RDB_64BITLEN = 0x81
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_AUX = 250
REDIS_RDB_OPCODE_RESIZEDB = 251
REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
REDIS_RDB_OPCODE_EXPIRETIME = 253
REDIS_RDB_OPCODE_SELECTDB = 254
REDIS_RDB_OPCODE_EOF = 255



REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4
REDIS_RDB_TYPE_ZSET_2 = 5  # ZSET version 2 with doubles stored in binary.
REDIS_RDB_TYPE_MODULE = 6
REDIS_RDB_TYPE_MODULE_2 = 7
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
REDIS_RDB_TYPE_HASH_ZIPLIST = 13
REDIS_RDB_TYPE_LIST_QUICKLIST = 14

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3


REDIS_RDB_MODULE_OPCODE_EOF = 0   # End of module value.
REDIS_RDB_MODULE_OPCODE_SINT = 1
REDIS_RDB_MODULE_OPCODE_UINT = 2
REDIS_RDB_MODULE_OPCODE_FLOAT = 3
REDIS_RDB_MODULE_OPCODE_DOUBLE = 4
REDIS_RDB_MODULE_OPCODE_STRING = 5


class RdbParser:
    """
    A Parser for Redis RDB Files
    """

    def __init__(self, key_filter=None):
        self._key = None
        self._pttl = None
        self._value = None
        self.version = None
        if key_filter is None:
            def matchall(x):
                return True

            key_filter = matchall

        self._filter = key_filter

    def parse(self, filename):
        """
        Parse a redis rdb dump file and yield key, serialized dump, ttl
        """
        with sys.stdin if filename == '-' else open(filename, "rb") as f:
            self.verify_magic_string(f.read(5))
            self.verify_version(f.read(4))
            while True:
                self._pttl = self._key = self._value = None
                data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS:
                    self._pttl = read_unsigned_long(f)
                    data_type = read_unsigned_char(f)
                elif data_type == REDIS_RDB_OPCODE_EXPIRETIME:
                    self._pttl = read_unsigned_int(f) * 1000
                    data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_SELECTDB:
                    self.read_length(f)
                    continue

                if data_type == REDIS_RDB_OPCODE_AUX:
                    self.read_string(f)
                    self.read_string(f)
                    continue

                if data_type == REDIS_RDB_OPCODE_RESIZEDB:
                    self.read_length(f)
                    self.read_length(f)
                    continue

                if data_type == REDIS_RDB_OPCODE_EOF:
                    return

                self.read_key_and_object(f, data_type)
                if self._filter(self._key):
                    yield self._key, self._value, self._pttl or 0

    def read_length_with_encoding(self, f, out):
        is_encoded = False
        bytes = []
        bytes.append(read_unsigned_char(f, out))
        enc_type = (bytes[0] & 0xC0) >> 6
        if enc_type == REDIS_RDB_ENCVAL:
            is_encoded = True
            length = bytes[0] & 0x3F
        elif enc_type == REDIS_RDB_6BITLEN:
            length = bytes[0] & 0x3F
        elif enc_type == REDIS_RDB_14BITLEN:
            bytes.append(read_unsigned_char(f, out))
            length = ((bytes[0] & 0x3F) << 8) | bytes[1]
        elif enc_type == REDIS_RDB_32BITLEN:
            length = read_unsigned_int_be(f, out)
        elif enc_type == REDIS_RDB_64BITLEN:
            length = read_unsigned_long_be(f, out)
        else:
            length = ntohl(f, out)
        return (length, is_encoded, bytes)

    def read_length(self, f, out=None):
        return self.read_length_with_encoding(f, out)[0]

    def read_key_and_object(self, f, data_type):
        self._key = self.read_string(f, decompress=True)
        self._value = self.read_object(f, data_type)

    def read_binary_double(self, f, out=None):
        read_bytes(f, 8, out)

    def read_float(self, f, out=None):
        dbl_length = read_unsigned_char(f, out)
        if dbl_length < 253:
            read_bytes(f, dbl_length, out)

    def read_string(self, f, out=None, decompress=False):
        tup = self.read_length_with_encoding(f, out)
        length = tup[0]
        is_encoded = tup[1]
        bytes_to_read = 0
        if is_encoded:
            if length == REDIS_RDB_ENC_INT8:
                bytes_to_read = 1
            elif length == REDIS_RDB_ENC_INT16:
                bytes_to_read = 2
            elif length == REDIS_RDB_ENC_INT32:
                bytes_to_read = 4
            elif length == REDIS_RDB_ENC_LZF:
                clen = self.read_length(f, out)
                l = self.read_length(f, out)
                if decompress:
                    return lzf_decompress(f.read(clen), l)
                else:
                    bytes_to_read = clen
        else:
            bytes_to_read = length

        return read_bytes(f, bytes_to_read, out)

    def read_object(self, f, enc_type):
        out = [struct.pack('B', enc_type)]
        skip_strings = 0
        if enc_type == REDIS_RDB_TYPE_STRING:
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST:
            skip_strings = self.read_length(f, out)
        elif enc_type == REDIS_RDB_TYPE_SET:
            skip_strings = self.read_length(f, out)
        elif enc_type == REDIS_RDB_TYPE_ZSET:
            skip_strings = self.read_length(f, out) * 2
        elif enc_type == REDIS_RDB_TYPE_ZSET_2:
            length = self.read_length(f, out)
            for x in range(length):
                self.read_string(f, out)
                self.read_binary_double(f, out)
        elif enc_type == REDIS_RDB_TYPE_HASH:
            skip_strings = self.read_length(f, out) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP:
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST:
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET:
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST:
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST:
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_QUICKLIST:
            skip_strings = self.read_length(f, out)
        else:
            raise Exception(
                'read_object',
                'Invalid object type %d for key %s' % (enc_type, self._key))
        for x in range(0, skip_strings):
            self.read_string(f, out)

        out.append(struct.pack('I', self.version))
        out.append(b'\x00')
        res = b''.join(out)
        checksum = crc64(res)
        return res + struct.pack('<Q', checksum)

    def verify_magic_string(self, magic_string):
        if magic_string != b'REDIS':
            raise Exception('verify_magic_string', 'Invalid File Format')

    def verify_version(self, version_str):
        version = int(version_str)
        self.version = version
        if version < 1 or version > 8:
            raise Exception('verify_version',
                            'Invalid RDB version number %d' % version)


def ntohl(f, out=None):
    val = read_unsigned_int(f, out)
    new_val = 0
    new_val = new_val | ((val & 0x000000ff) << 24)
    new_val = new_val | ((val & 0xff000000) >> 24)
    new_val = new_val | ((val & 0x0000ff00) << 8)
    new_val = new_val | ((val & 0x00ff0000) >> 8)
    return new_val


def read_unsigned_char(f, out=None):
    return struct.unpack('B', read_bytes(f, 1, out))[0]

def read_unsigned_int(f, out=None):
    return struct.unpack('I', read_bytes(f, 4, out))[0]

def read_unsigned_int_be(f, out=None):
    return struct.unpack('>I', read_bytes(f, 4, out))[0]

def read_unsigned_long_be(f, out=None):
    return struct.unpack('>Q', read_bytes(f, 8, out))[0]

def read_unsigned_long(f, out=None):
    return struct.unpack('Q', read_bytes(f, 8, out))[0]


def read_bytes(f, l, out=None):
    _buf = f.read(l)
    if out is not None:
        out.append(_buf)
    return _buf


def parse_rdb(filename, key_filter=None):
    parser = RdbParser(key_filter=key_filter)
    return parser.parse(filename)


def lzf_decompress(compressed, expected_length):
    in_stream = bytearray(compressed)
    in_len = len(in_stream)
    in_index = 0
    out_stream = bytearray()
    out_index = 0

    while in_index < in_len:
        ctrl = in_stream[in_index]
        if not isinstance(ctrl, int):
            raise Exception('lzf_decompress',
                            'ctrl should be a number %s' % str(ctrl))
        in_index = in_index + 1
        if ctrl < 32:
            for x in range(0, ctrl + 1):
                out_stream.append(in_stream[in_index])
                in_index += 1
                out_index += 1
        else:
            length = ctrl >> 5
            if length == 7:
                length += in_stream[in_index]
                in_index += 1

            ref = out_index - ((ctrl & 0x1f) << 8) - in_stream[in_index] - 1
            in_index += 1
            for x in range(0, length + 2):
                out_stream.append(out_stream[ref])
                ref += 1
                out_index += 1
    if len(out_stream) != expected_length:
        raise Exception(
            'lzf_decompress',
            'Expected lengths do not match %d != %d' % (
                len(out_stream), expected_length))
    return str(out_stream)
