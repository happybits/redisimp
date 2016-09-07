# Borrowed from rdb-tools

import struct
from .crc64 import crc64

__all__ = ['parse_rdb']


REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 2
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
REDIS_RDB_OPCODE_EXPIRETIME = 253
REDIS_RDB_OPCODE_SELECTDB = 254
REDIS_RDB_OPCODE_EOF = 255

REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
REDIS_RDB_TYPE_HASH_ZIPLIST = 13

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3


class RdbParser:
    """
    A Parser for Redis RDB Files
    """

    def __init__(self, key_filter=None):
        self._key = None
        self._pttl = None
        self._value = None
        if key_filter is None:
            def matchall(x):
                return True

            key_filter = matchall

        self._filter = key_filter

    def parse(self, filename):
        """
        Parse a redis rdb dump file and yield key, serialized dump, ttl
        """
        with open(filename, "rb") as f:
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

                if data_type == REDIS_RDB_OPCODE_EOF:
                    return

                self.read_key_and_object(f, data_type)
                if self._filter(self._key):
                    yield self._key, self._value, self._pttl or 0

    def read_length_with_encoding(self, f, out):
        length = 0
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
        else:
            length = ntohl(f, out)
        return (length, is_encoded, bytes)

    def read_length(self, f, out=None):
        return self.read_length_with_encoding(f, out)[0]

    def read_key_and_object(self, f, data_type):
        self._key = self.read_string(f)
        self._value = self.read_object(f, data_type)

    def read_string(self, f, out=None):
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
                self.read_length(f, out)
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
        else:
            raise Exception(
                'read_object',
                'Invalid object type %d for key %s' % (enc_type, self._key))
        for x in xrange(0, skip_strings):
            self.read_string(f, out)

        out.append("\x06\x00")

        res = b''.join(out)
        checksum = crc64(res)
        return res + struct.pack('<Q', checksum)

    def verify_magic_string(self, magic_string):
        if magic_string != 'REDIS':
            raise Exception('verify_magic_string', 'Invalid File Format')

    def verify_version(self, version_str):
        version = int(version_str)
        if version < 1 or version > 7:
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