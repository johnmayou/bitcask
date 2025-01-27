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

    def serialize(epoch:, key:, val:)
      key_type = DATA_TYPE[key.class]
      key_b = pack(key, key_type)

      val_type = DATA_TYPE[val.class]
      val_b = pack(val, val_type)

      header_b = [epoch, key_b.length, val_b.length, key_type, val_type].pack(HEADER_FORMAT)
      crc32_b = [Zlib.crc32(header_b + key_b + val_b)].pack(CRC32_FORMAT)

      crc32_b + header_b + key_b + val_b
    end

    def deserialize(data)
      crc32_b = data[..crc32_offset - 1]
      crc32 = crc32_b.unpack1(CRC32_FORMAT)
      return 0, '', '' unless crc32 == crc32(data[crc32_offset..])

      header = data[crc32_offset..crc32_header_offset - 1].unpack(HEADER_FORMAT)
      epoch, key_sz, _val_sz, key_type, val_type = header

      key_b = data[crc32_header_offset..crc32_header_offset + key_sz - 1]
      key = unpack(key_b, key_type)

      val_b = data[crc32_header_offset + key_sz..]
      val = unpack(val_b, val_type)

      [epoch, key, val]
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

    def crc32(bytes)
      Zlib.crc32(bytes)
    end

    def crc32_header_offset
      CRC32_SIZE + HEADER_SIZE
    end

    def crc32_offset
      CRC32_SIZE
    end

    def header_offset
      HEADER_SIZE
    end
  end

  class DiskStore
    include Serializer

    KeyStruct = Struct.new(:write_pos, :log_size, :key)

    def initialize(file)
      @file = File.open(file, 'a+b')
      @write_pos = 0
      @key_map = {}

      init_key_map
    end

    def get(key)
      key_struct = @key_map[key]
      return '' if key_struct.nil?

      @file.seek(key_struct.write_pos)
      _epoch, _key, val = deserialize(@file.read(key_struct.log_size))

      val
    end

    def put(key, val)
      data = serialize(epoch: Time.now.to_i, key:, val:)
      @key_map[key] = KeyStruct.new(write_pos: @write_pos, log_size: data.length, key:)
      @file.write(data)
      @file.flush
      @write_pos += data.length

      nil
    end

    def keys
      @key_map.keys
    end

    def size
      @key_map.length
    end

    def close
      @file.close
    end

    private

    def init_key_map
      while (crc32_header_b = @file.read(crc32_header_offset))
        crc32_b = crc32_header_b[..crc32_offset - 1]
        crc32 = crc32_b.unpack1(Serializer::CRC32_FORMAT)

        header_b = crc32_header_b[crc32_offset..crc32_header_offset - 1]
        header = header_b.unpack(Serializer::HEADER_FORMAT)

        _epoch, key_sz, val_sz, key_type, _val_type = header

        key_b = @file.read(key_sz)
        key = unpack(key_b, key_type)
        
        val_b = @file.read(val_sz)

        raise StandardError, 'file corrupted' unless crc32 == crc32(header_b + key_b + val_b)

        log_size = crc32_header_offset + key_sz + val_sz
        @key_map[key] = KeyStruct.new(write_pos: @write_pos, log_size:, key:)
        @write_pos += log_size
      end
    end
  end
end