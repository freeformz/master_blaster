module SilentLogger
  def self.log(*args,&blk)
    yield if blk
  end
end
