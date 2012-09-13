require_relative '../lib/master_blaster'

module SilentLogger
  def self.log(*args,&blk)
    yield if blk
  end
end

describe AtomicHash do
  let(:ah) { AtomicHash.new }

  before do
    ah.logger = SilentLogger
  end

  context '#first_with' do
    context 'when there are no members with the key or value' do
      it 'should return nil' do
        ah.first_with(:state, 'bar').should == nil
      end
    end

    context 'when there is a member with the key and value' do
      before do
        ah.update('123', {:state => 'bar'})
      end

      it 'should return nil' do
        ah.first_with(:state, 'foo').should == nil
      end
    end

    context 'when there is a worker in the state I am asking for' do
      before do
        ah.update('123', {:state => 'foo'})
      end

      it 'should return the right item' do
        ah.first_with(:state, 'foo').should == ["123", {:state=>"foo"}]
      end
    end
  end

  context '#snapshot' do
    it 'should return a normal hash, which is a snapshot in time' do
      ah.snapshot.should == {}
      ah.update('123',{:state => 'foo'})
      ah.snapshot.should == {'123' => {state: 'foo'}}
      ah.update('456',{:state => 'foo'})
      ah.snapshot.should == {'123' => {state: 'foo'}, '456' => {state: 'foo'}}
    end

  end

  context '#delete' do
    context 'when there are no workers' do
      it 'should work' do
        ah.snapshot.should == {}
        ah.update('123',{}).should == {}
        ah.snapshot.should == {'123' => {}}
      end
    end

    context 'when there is a different worker than the one we want to remove' do
      it 'should work' do
        ah.update('123', {:state => 'foo'})
        ah.snapshot.should == {'123' => {state: 'foo'}}
        ah.delete('456').should == nil
        ah.snapshot.should == {'123' => {state: 'foo'}}
      end
    end

    context 'when there are multple workers and we remove one of them' do
      it 'should work' do
        ah.update('123', {:state => 'foo'})
        ah.update('456', {:state => 'bar'})
        ah.snapshot.should == {'123' => {state: 'foo'}, '456' => {state: 'bar'}}
        ah.delete('456').should == {state: 'bar'}
        ah.snapshot.should == {'123' => {state: 'foo'}}
      end
    end
  end

end
