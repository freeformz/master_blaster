#!/usr/bin/env ruby

require_relative '../lib/postgres_connection'
require_relative '../lib/silent_logger'
require 'forwardable'

class Repl
  extend Forwardable
  include PostgresConnection

  attr :logger

  def_delegators :logger, :log

  def initialize(url)
    @logger = SilentLogger
    setup_pg_connection(url, 'mb_repl')
    listen_for 'mb_admin_response'
  end

  def run
    puts "enter commands"
    last_response_pid = nil
    Thread.new do
      loop do
        conn.wait_for_notify do |event, pid, payload|
          unless last_response_pid == pid
            puts "< ** Received response from pid: #{pid} **"
            last_response_pid = pid
          end
          puts "< " << payload
        end
      end
    end

    loop do
      command = STDIN.gets.gsub(/\W/,'')
      puts "> " << command
      notify 'mb_admin', command
    end
  end
end

r = Repl.new(ENV['DATABASE_URL'])
r.run

