require 'zlib'

module Bitcask
  module Serializer

    CRC32_FORMAT = 'L<'
    CRC32_SIZE = 4

    HEADER_FORMAT = 'L<L<L<S<S<'
    HEADER_SIZE = 16

    DATA_TYPE = {
      Integer => 1,
      Float => 2,
      String => 3,
    }.freeze

    DATA_TYPE_LOOK_UP = {
      DATA_TYPE[Integer] => Integer,
      DATA_TYPE[Float] => Float,
      DATA_TYPE[String] => String,
    }

    DATA_TYPE_DIRECTIVE = {
      DATA_TYPE[Integer] => 'q<',
      DATA_TYPE[Float] => 'E',
    }

    def serialize(epoch:, key:, value:)
      key_type = DATA_TYPE[key.class]
      key_bytes = pack(key, key_type)

      value_type = DATA_TYPE[value.class]
      value_bytes = pack(value, value_type)

      header = [epoch, key_bytes.length, value_bytes.length, key_type, value_type].pack(HEADER_FORMAT)
      crc32 = [Zlib.crc32(header + key_bytes + value_bytes)].pack(CRC32_FORMAT)

      crc32 + header + key_bytes + value_bytes
    end

    def deserialize(data)
      return 0, '', '' unless crc32_valid?(data)

      header = data[CRC32_SIZE..meta_offset - 1].unpack(HEADER_FORMAT)
      epoch, key_sz, _value_sz, key_type, value_type = header

      key_bytes = data[meta_offset..meta_offset + key_sz - 1]
      key = unpack(key_bytes, key_type)

      value_bytes = data[meta_offset + key_sz..]
      value = unpack(value_bytes, value_type)

      [epoch, key, value]
    end

    private

    def pack(data, data_type)
      case data_type
      when DATA_TYPE[Integer], DATA_TYPE[Float]
        [data].pack(DATA_TYPE_DIRECTIVE[data_type])
      when DATA_TYPE[String]
        data.encode('utf-8')
      else
        raise ArgumentError, "Invalid data_type: #{data_type}"
      end
    end

    def unpack(data, data_type)
      case data_type
      when DATA_TYPE[Integer], DATA_TYPE[Float]
        data.unpack1(DATA_TYPE_DIRECTIVE[data_type])
      when DATA_TYPE[String]
        data
      else
        raise ArgumentError, "Invalid data_type: #{data_type}"
      end
    end

    def crc32_valid?(data)
      crc32 = data[..CRC32_SIZE - 1].unpack1(CRC32_FORMAT)
      crc32 == Zlib.crc32(data[CRC32_SIZE..])
    end

    def meta_offset
      CRC32_SIZE + HEADER_SIZE
    end
  end

  class DiskStore
    include Serializer

    def initialize(file = 'bitcask.db')
      @file = File.open(file, 'a+b')
      @write_pos = 0
      @key_map = {}
    end

    def get(key)
      key_hash = @key_map[key]
      return '' if key_hash.nil?

      @file.seek(key_hash[:write_pos])
      _epoch, _key, value = deserialize(@file.read(key_hash[:log_size]))

      value
    end

    def put(key, value)
      data = serialize(epoch: Time.now.to_i, key: key, value: value)
      @key_map[key] = {write_pos: @write_pos, log_size: data.length, key: key}
      @file.write(data)
      @file.flush
      @write_pos += data.length

      nil
    end
  end
end