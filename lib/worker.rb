require_relative 'postgres_connection'

require 'uri'
require 'securerandom'
require 'forwardable'
require 'thread'

require 'scrolls'

class Worker
  extend Forwardable
  include PostgresConnection

  attr_accessor :logger

  def_delegator :logger, :log

  def initialize(url)
    @logger = Scrolls
    log states: @states
    @uuid = "worker_#{SecureRandom.uuid.gsub('-','_')}"
    setup_pg_connection(url, @uuid)
    listen_for @uuid
    listen_for "worker_terminate"
    set_state :worker_starting
    trap('TERM') { terminate }
    trap('INT')  { terminate }
  end

  def work(job)
    log(class: self.class, fn: :work) do
      log(class: self.class, fn: :work, doing: :work)
    end
  end

  def run
    loop do
      job = get_job
      log(class: Worker, fn: :run, job_received: job)
      set_state :worker_running_job
      work(job)
    end
  end

  def get_job
    set_state :worker_waiting_for_job
    conn.wait_for_notify do |event, pid, payload|
      if event == @uuid && payload =~ /^JOB/
        log(job: true, event: event, pid: pid, payload: payload)
        payload.gsub(/^JOB:/,'')
      else
        handle_other_event(event, pid, payload)
      end
    end
  end

  def terminate(pid=nil, payload=nil)
    notify "worker_shutting_down", @uuid
    conn.finish
    exit
  end

  private

  def handle_other_event(event, pid, payload)
    unless event == 'worker_terminate'
      log(class: self.class, fn: :handle_other_event, unknown_event: event, pid: pid, payload: payload)
    end
    terminate
  end

  def set_state(new_state)
    log(class: self.class, method: :set_state, old_state: @state, new_state: new_state) do
      notify new_state, @uuid
      wait_for_ack(new_state)
      @state = new_state
    end
  end

  def wait_for_ack(state)
    # FIXME: If we don't get an ACK for the state, blow up or something
    log(class: self.class, fn: :wait_for_ack) do
      conn.wait_for_notify do |event, pid, payload|
        if event == @uuid && payload == "ACK:#{state}"
          log(ack: true, current_state: @state, target_state: state)
        else
          handle_other_event(event, pid, payload)
        end
      end
    end
  end
end
