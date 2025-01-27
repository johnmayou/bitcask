require 'minitest/autorun'
require_relative 'bitcask.rb'

class TestBitcask < Minitest::Test
  def setup
    @file = 'test.db'
    @store = Bitcask::DiskStore.new(@file)
  end

  def teardown
    File.delete(@file)
  end

  def test_returns_empty_string
    assert_equal '', @store.get('random')
  end

  def test_string
    @store.put('key', 'value')
    assert_equal 'value', @store.get('key')
  end

  def test_integer
    @store.put(1, 2)
    assert_equal 2, @store.get(1)
  end

  def test_float
    @store.put(1.2, 2.2)
    assert_equal 2.2, @store.get(1.2)
  end

  def test_key_override
    @store = Bitcask::DiskStore.new(@file)
    @store.put('key', 'value')
    @store.put('key', 'value2')
    assert_equal 'value2', @store.get('key')
  end

  def test_multiple_keys
    @store = Bitcask::DiskStore.new(@file)
    @store.put('key1', 'value1')
    @store.put('key2', 'value2')
    @store.put('key3', 'value3')
    assert_equal ['value1', 'value2', 'value3'],
      [@store.get('key1'), @store.get('key2'), @store.get('key3')]
  end
end