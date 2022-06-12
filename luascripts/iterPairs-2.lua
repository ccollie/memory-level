--[[
 redis.eval('iterPairs', 2, zkey, hkey, reverse, min, max[, count])

 Make a range query by lex on an ordered map and return the pairs.

 `redis-cli EVAL "$(cat iterPairs.lua)" 2 _redisdown_test_db_:10 ZRANGEBYLEX - +`
]]
local zKey = KEYS[1]
local hKey = KEYS[2]
local reverse = tonumber(ARGV[1] or 0)
local command = (reverse == 1) and 'zrevrangebylex' or 'zrangebylex'
local min = ARGV[2]
local max = ARGV[3]
local count = tonumber(ARGV[4] or 10)

local keys = redis.call(command, zKey, min, max, 'LIMIT', 0, count)
if #keys == 0 then
  return keys
end
local values = redis.call('hmget', hKey, unpack(keys))
local result = {}
for i,v in ipairs(keys) do
  result[i*2-1] = v
  result[i*2] = values[i]
end
return result
