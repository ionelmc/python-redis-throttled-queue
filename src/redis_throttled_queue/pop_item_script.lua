--[[
This script takes no KEYS arguments.

ARGV arguments:
    PREFIX: Key prefix to use for any generated key.
    WINDOW: The current window. Usually the current second or minute. Could in theory be a composite of the current minute and other data.
    LIMIT: The strict ITEM limit for the WINDOW. This script will not return anything if the limit would be reached.
    RESOLUTION: Seconds to use as WINDOW key expiry. If you want to have 1 minute windows use value '60'.

Typical key structure (for every ITEM returned M is incremented, but only while M < limit):

    PREFIX:usage:WINDOW - zlist of (M, NAME)
    PREFIX:queue:NAME - zset of ITEM
    PREFIX:total - int of total ITEM count
    PREFIX:names - zset of NAME
]]
if #ARGV ~= 4 then
    error('POP_ITEM_SCRIPT expected 4 arguments, but got ' .. #ARGV .. ' arguments!')
end
local PREFIX, WINDOW, LIMIT, RESOLUTION = unpack(ARGV)
LIMIT = tonumber(LIMIT)
local names_key = PREFIX .. ':names'
local total_key = PREFIX .. ':total'
local usage_key = PREFIX .. ':usage:' .. WINDOW

local usage_count = redis.call('ZCARD', usage_key)
if usage_count == 0 then
    redis.call('COPY', names_key, usage_key, 'REPLACE')
end
redis.call('EXPIRE', usage_key, RESOLUTION)

local names = redis.call('ZRANGE', usage_key, 0, '(' .. LIMIT, 'BYSCORE')
for _, name in ipairs(names) do
    local queue_key = PREFIX .. ':queue:' .. name
    local highest_item = redis.call('ZPOPMAX', queue_key)
    if #highest_item ~= 0 then
        local value = highest_item[1]
        redis.call('ZINCRBY', usage_key, 1, name)
        redis.call('DECR', total_key)
        return value
    else
        redis.call('ZREM', names_key, name)
    end
end
