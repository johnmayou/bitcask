require 'minitest/autorun'
require_relative 'bitcask.rb'

class TestBitcask < Minitest::Test
  def setup
    @filename = 'test.db'
    @store = Bitcask::DiskStore.new(@filename)
  end

  def teardown
    @store.close
    File.delete(@filename)
  end

  def test_returns_empty_string
    assert_equal '', @store.get('random')
  end

  def test_string
    @store.put('key', 'val')
    assert_equal 'val', @store.get('key')
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
    @store.put('key', 'val')
    @store.put('key', 'val2')
    assert_equal 'val2', @store.get('key')
  end

  def test_multiple_keys
    @store.put('key1', 'val1')
    @store.put('key2', 'val2')
    @store.put('key3', 'val3')
    assert_equal ['val1', 'val2', 'val3'],
      [@store.get('key1'), @store.get('key2'), @store.get('key3')]
  end

  def test_existing_database
    @store.put('key1', 'val1')
    @store.put('key2', 'val2')
    @store.close
    @store = Bitcask::DiskStore.new(@filename)
    assert_equal ['val1', 'val2'],
      [@store.get('key1'), @store.get('key2')]
  end
end