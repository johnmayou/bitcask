require 'benchmark'
require 'benchmark/ips'
require_relative 'bitcask.rb'

store = Bitcask::DiskStore.new('benchmark.db')

value_small  = 'A' * 100    # 100-byte
value_medium = 'A' * 1_000  # 1Kb
value_large  = 'A' * 10_000 # 10Kb

keys_1k   = Array.new(1_000) { "1_000 #{it}" }
keys_10k  = Array.new(10_000) { "10_000 #{it}" }
keys_100k = Array.new(100_000) { "100_000 #{it}" }

Benchmark.bm(15) do |bm|
  bm.report('put (1k records)') do
    keys_1k.each { store.put(it, value_small) }
  end

  bm.report('put (10k records)') do
    keys_10k.each { store.put(it, value_small) }
  end

  bm.report('put (100k records)') do
    keys_100k.each { store.put(it, value_small) }
  end

  bm.report('get (1k records)') do
    keys_1k.each { store.get(it) }
  end

  bm.report('get (10k records)') do
    keys_10k.each { store.get(it) }
  end

  bm.report('get (100k records)') do
    keys_100k.each { store.get(it) }
  end
end

Benchmark.ips do |bm|
  bm.report("put (#{value_small.length} bytes)") do
    100.times { store.put("#{value_small.length}#{it}", value_small) }
  end

  bm.report("get (#{value_small.length} bytes)") do
    100.times { store.get("#{value_small.length}#{it}") }
  end

  bm.report("put (#{value_medium.length} bytes)") do
    100.times { store.put("#{value_medium.length}#{it}", value_medium) }
  end

  bm.report("get (#{value_medium.length} bytes)") do
    100.times { store.get("#{value_medium.length}#{it}") }
  end

  bm.report("put (#{value_large.length} bytes)") do
    100.times { store.put("#{value_large.length}#{it}", value_large) }
  end

  bm.report("get (#{value_large.length} bytes)") do
    100.times { store.get("#{value_large.length}#{it}") }
  end
end

File.delete('benchmark.db')