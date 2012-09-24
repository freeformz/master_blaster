require 'forwardable'
require 'scrolls'

class JobFeeder
  extend Forwardable

  def_delegators :@logger, :log

  def initialize(conn)
    trap('TERM') { terminate }
    trap('INT')  { terminate }
    @logger = Scrolls
    @conn = conn
    @conn.set_application_name('job_feeder')
    @start = 0
  end

  def run
    loop do
      get_job.tap do |job|
        log(class: self.class, feeding_job: job)
        @conn.notify(:feed_job, job)
      end
    end
  end

  private

  def get_job
    @start += 1
  end

  def terminate(pid=nil, payload=nil)
    @conn.notify(:feeder_shutting_down)
    @conn.finish
    exit
  end
end
