#!/usr/bin/env ruby

=begin
gem install memcache-client
=end
require 'memcache'
require 'pp'
require 'json'
require 'net/http'

THREADS = 1
DOCKER_HOST = ENV["DOCKER_HOST"].match(/\d+\.\d+\.\d+\.\d+/).to_s #"192.168.99.100"


count = 12
begin
  resp = Net::HTTP.get(URI("http://localhost:10001/"))
  servers = JSON.parse(resp)

  servers = servers.values.map{|s| "#{DOCKER_HOST}:#{s['Port']}" }
  pp servers
  #servers = servers.values.map{|s| "#{s['Hostname']}:#{s['Port']}" }
  client = MemCache.new(servers)
  client.set('test', 1)

  length = 4096

  (0..THREADS).map do |thread_id|
    Thread.new do
      100.times do |n|
        key = "#{thread_id}-#{count}-#{n}"
        val = rand(36**length).to_s(36)
        client.set(key, val)
      end
    end
  end.map(&:join)
  

  count += 1
  sleep(2)
end while(true)
