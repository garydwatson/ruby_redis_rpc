require 'securerandom'
require 'redis'

class Message
  attr_reader :payload, :guid

  def initialize(*args)
    @guid = SecureRandom.uuid
    @payload = args
  end
end

class RPCExceptionContainer
  attr_reader :saved_exception

  def initialize(exception)
    @saved_exception = exception
  end
end

def rsend(key, *args)
  @r ||= Redis.new
  Message.new(*args).tap do |message|
    @r.rpush(key, Marshal.dump(message))
    Marshal.load(@r.blpop(message.guid)[1]).tap do |return_value|
      if return_value.class == RPCExceptionContainer
        raise return_value.saved_exception
      else
        return return_value
      end
    end
  end
end

def rrecv(key)
  @keys ||= Hash.new
  if(!@keys.has_key?(key))
    r = Redis.new
    Thread.new do
      loop do
        Marshal.load(r.blpop(key)[1]).tap do |message|
          begin
            return_value = yield(message.payload)
            r.rpush(message.guid, Marshal.dump(return_value))
            r.expire(message.guid, 30)
          rescue
            r.rpush(message.guid, Marshal.dump(RPCExceptionContainer.new($!)))
            r.expire(message.guid, 30)
          end
        end
      end
    end
    @keys[key] = true
  else
    raise "You Called rrecv twice on the same key, this is almost certainly not what you intended"
  end
end

rrecv(:hello) do |args|
  puts "Hello #{args.reduce(:+)}"
  raise "what what"
  "success"
end
x = rsend(:hello, "hi there", "nobody") rescue "bobby farquard"
puts x

rrecv(:there) do |args|
  puts "There #{args.reduce(:+)}"
  "success"
end
y = rsend(:there, "something else")
puts y


x = rsend(:hello, "hi there", "nobody") rescue "bobby farquard again"
puts x

