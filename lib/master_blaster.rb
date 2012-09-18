require 'thread'
require 'forwardable'

require 'uri'
require 'scrolls'

class JobFeeder
  def initialize(queue)
    @queue = queue
    @start = 1
  end

  def run
    loop do
      @queue << (@start += 1)
      sleep 1
    end
  end
end

class MasterBlaster
  extend Forwardable

  def_delegators :@logger, :log

  def initialize(conn, queue)
    @logger = Scrolls
    @conn = conn
    @conn.set_application_name('master_blaster')
    @queue = queue
    terminate_workers
    @workers = {}
  end

  def run
    @conn.listen_for :worker_starting
    @conn.listen_for :worker_waiting_for_job
    @conn.listen_for :worker_shutting_down
    @conn.listen_for :worker_running_job
    @conn.listen_for :mb_admin
    run_workers
    work
  end

  private

  def work
    log(class: self.class, fn: :work) do
      loop do
        @conn.wait_for_notify(0.1) do |event, pid, payload|
          log(class: self.class, fn: :work, recieved_event: event, from: pid, payload: payload)
          case event
          when 'worker_starting', 'worker_waiting_for_job', 'worker_running_job'
            uuid = uuid_from_payload(payload)
            update_worker_state(uuid, event)
            ack(payload, event)
          when 'worker_shutting_down'
            @workers.delete(payload)
            @conn.unlisten payload
          when 'mb_admin'
            next if payload.nil?
            input = payload.split(':')
            begin
              send("admin_#{input.first}".to_sym,*input[1..-1])
            rescue NoMethodError => e
                send_admin_response "Unknown command. Try 'help'"
            rescue => e
              send_admin_response e.message.to_s.gsub(/\'/,'')
            end
          else
            log(class: self.class, fn: :work, unknown_event: true)
            exit
          end
        end
        if job = get_job
          log(class: self.class, fn: :work, job_acquired: job)
          if worker_id = get_a_worker_id_in_state("worker_waiting_for_job")
            log(class: self.class, fn: :work, worker_acquired: worker_id, job: job)
            update_worker_state(worker_id, 'worker_sent_job')
            @conn.notify worker_id, "JOB:#{job}"
          else
            log(class: self.class, fn: :work, re_enqueue: true, job: job)
            @queue << job
          end
        end
      end
    end
  end

  def send_admin_response(msg)
    @conn.notify "mb_admin_response", msg
  end

  def admin_help
    send_admin_response "help: help text"
    send_admin_response "ping: responds with pong"
    send_admin_response "queue_length: reports the current queue length"
    send_admin_response "workers: reports the current worker info"
  end

  def admin_ping
    send_admin_response "pong"
  end

  def admin_queue_length
    send_admin_response @queue.length
  end

  def admin_workers
    if @workers.length == 0 
      send_admin_response "none"
    end
    @workers.each do |worker|
      send_admin_response worker.to_s
    end
  end

  def update_worker_state(id, state, time=Time.now)
    @workers[id] = {:state => state, :when => time}
  end

  def get_a_worker_id_in_state(state)
    worker = @workers.select { |key, attrs| attrs[:state] == state }.to_a.sample
    worker.nil? ? nil : worker.first
  end

  def uuid_from_payload(payload)
    payload.split(':').first
  end

  def get_job
    log(class: self.class, fn: :get_job) do
      begin
        @queue.pop(true)
      rescue ThreadError => e
        return nil if e.message == 'queue_empty'
      end
    end
  end

  def ack(uuid, event)
    @conn.notify uuid, "ACK:#{event}"
  end

  def run_workers
    #do stuff here to start workers on heroku
  end

  def terminate_workers
    @conn.notify 'worker_terminate'
  end
end
