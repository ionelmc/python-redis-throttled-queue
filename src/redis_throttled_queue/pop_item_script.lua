--[[
This script takes no KEYS arguments.

ARGV arguments:
    PREFIX: Key prefix to use for any generated key.
    WINDOW: The current window. Usually the current second or minute. Could in theory be a composite of the current minute and other data.
    LIMIT: The strict ITEM limit for the WINDOW. This script will not return anything if the limit would be reached.
    RESOLUTION: Seconds to use as WINDOW key expiry. If you want to have 1 minute windows use value "60".

Typical key structure (for every ITEM returned M is incremented, but only while M < limit):

    PREFIX:usage:WINDOW - zlist of (M, NAME)
    PREFIX:queue:NAME - list of ITEM
]]
if #ARGV ~= 4 then
    error("POP_ITEM_SCRIPT expected 4 arguments, but got " .. #ARGV .. " arguments!")
end
local PREFIX, WINDOW, LIMIT, RESOLUTION = unpack(ARGV)
LIMIT = tonumber(LIMIT)
local usage_key = PREFIX .. ":usage:" .. WINDOW
local total_key = PREFIX .. ":total"
local names_key = PREFIX .. ":names"

local name = redis.call("ZRANGE", usage_key, 0, "(" .. LIMIT, "BYSCORE", "LIMIT", 0, 1)[1]
redis.call('SET', 'DEBUG', 'ZRANGE ' .. usage_key .. ' => ' .. tostring(name))
if name ~= nil then
    local queue_key = PREFIX .. ":queue:" .. name
    local result = redis.call("ZPOPMAX", queue_key)
    redis.call('SET', 'DEBUG', 'ZPOPMAX ' .. queue_key .. ' => #' .. tostring(#result))
    if #result ~= 0 then
        redis.call("ZINCRBY", usage_key, 1, name)
        redis.call("EXPIRE", usage_key, RESOLUTION)
        redis.call("DECR", total_key)
        redis.call('SET', 'DEBUG', '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>   returning: ' .. tostring(result[1]))
        return result[1]
    end
end


local cursor = "0"
repeat
    local result = redis.call("SSCAN", names_key, cursor, "COUNT", 1000)
    redis.call('SET', 'DEBUG', 'SSCAN ' .. names_key .. ' ' .. cursor .. " COUNT 1000 => @" .. result[1] .. ', ' .. type(result[2]))
    local names = result[2]
    if names ~= nil then
        for _, name in ipairs(names) do
            local queue_key = PREFIX .. ":queue:" .. name
            if redis.call('EXISTS', queue_key) then
                redis.call("ZINCRBY", usage_key, 0, name)
            else
                redis.call("SREM", names_key, name)
            end
        end
    end
    cursor = result[1]
until cursor == "0"

local name = redis.call("ZRANGE", usage_key, 0, "(" .. LIMIT, "BYSCORE", "LIMIT", 0, 1)[1]
redis.call('SET', 'DEBUG', 'ZRANGE ' .. usage_key .. ' => ' .. tostring(name))
if name ~= nil then
    local queue_key = PREFIX .. ":queue:" .. name
    local result = redis.call("ZPOPMAX", queue_key)
    redis.call('SET', 'DEBUG', 'ZPOPMAX ' .. queue_key .. ' => #' .. tostring(#result))
    if #result ~= 0 then
        redis.call("ZINCRBY", usage_key, 1, name)
        redis.call("EXPIRE", usage_key, RESOLUTION)
        redis.call("DECR", total_key)
        redis.call('SET', 'DEBUG', '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>   returning: ' .. tostring(result[1]))
        return result[1]
    end
end
