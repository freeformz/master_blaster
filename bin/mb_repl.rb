#!/usr/bin/env ruby

require_relative '../lib/postgres_connection'
require_relative '../lib/silent_logger'
require 'forwardable'

class Repl
  extend Forwardable

  def_delegators :@logger, :log

  def initialize(conn)
    @logger = SilentLogger
    @conn = conn
    @conn.set_application_name('mb_repl')
    @conn.listen_for 'mb_admin_response'
  end

  def run
    puts "enter commands"
    last_response_pid = nil
    Thread.new do
      loop do
        @conn.wait_for_notify do |event, pid, payload|
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
      @conn.notify 'mb_admin', command
    end
  end
end

pgconn = PostgresConnectionByURL.new(ENV['DATABASE_URL'])
pgconn.logger = SilentLogger

r = Repl.new(pgconn)
r.run

