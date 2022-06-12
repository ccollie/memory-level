--[[
 redis.eval('iterValues', 2, zsetKey, hashKey, reverse, max, min, count)
 Make a reverse range by lex on an order map and return the values
 Note that the last returned value is the key of the last value. Used for the next iteration.
 `redis-cli EVAL "$(cat zhrevvalues.lua)" 2 _redisdown_test_db_:10 - +`
]]
local zsetKey = KEYS[1]
local hashKey = KEYS[2]
local reverse = tonumber(ARGV[1] or 0)
local min = ARGV[2]
local max = ARGV[3]
local count = tonumber(ARGV[4] or 10)

local command = (reverse == 1) and 'zrevrangebylex' or 'zrangebylex'

local keys = redis.call(command, zsetKey, min, max, 'LIMIT', 0, count)
if #keys == 0 then
  return keys
end
local values = redis.call('hmget', hashKey, unpack(keys))
values[#values+1] = keys[#keys]
return values
