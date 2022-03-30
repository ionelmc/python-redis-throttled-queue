--[[
This script takes no KEYS arguments.

ARGV arguments:
    PREFIX: Key prefix to use for any generated key.
    WINDOW: The current window. Usually the current second or minute. Could in theory be a composite of the current minute and other data.
    LIMIT: The strict ITEM limit for the WINDOW. This script will not return anything if the limit would be reached.
    RESOLUTION: Seconds to use as WINDOW key expiry. If you want to have 1 minute windows use value "60".

Typical key structure (for every ITEM returned M is incremented, but only while M < limit):

    PREFIX:usage:WINDOW - zlist of:
        (M, NAME)
    PREFIX:queue:NAME - list of:
        ITEM
]]
if #ARGV ~= 4 then
    error("POP_ITEM_SCRIPT expected 4 arguments, but got " .. #ARGV .. " arguments!")
end
local PREFIX, WINDOW, LIMIT, RESOLUTION = unpack(ARGV)
LIMIT = tonumber(LIMIT)
local usage_zset_key = PREFIX .. ":usage:" .. WINDOW
local name = redis.call("ZRANGE", usage_zset_key, 0, "(" .. LIMIT, "BYSCORE", "LIMIT", 0, 1)[1]
if name ~= nil then
    local queue_list = PREFIX .. ":queue:" .. name
    local value = redis.call("ZPOPMAX", queue_list)
    if value ~= nil then
        redis.call("ZINCRBY", usage_zset_key, 1, name)
        redis.call("EXPIRE", usage_zset_key, RESOLUTION)
        return value[1]
    end
end
local cursor = "0"
local queue_list_key_pattern = PREFIX .. ":queue:*"
local queue_list_key_prefix_len = #queue_list_key_pattern
repeat
    local result = redis.pcall("SCAN", cursor, "MATCH", queue_list_key_pattern, "COUNT", 1000)
    local queue_list_keys = result[2]
    if queue_list_keys ~= nil then
        for _, queue_list_key in ipairs(queue_list_keys) do
            local name = string.sub(queue_list_key, queue_list_key_prefix_len, -1)
            local score = redis.call("ZSCORE", usage_zset_key, name)
            redis.call('set', 'debug', tostring(score))
            if tonumber(score or 0) < LIMIT then
                local value = redis.call("ZPOPMAX", queue_list_key)
                if value ~= nil then
                    redis.call("ZINCRBY", usage_zset_key, 1, name)
                    redis.call("EXPIRE", usage_zset_key, RESOLUTION)
                    return value[1]
                end
            end
        end
    end
    cursor = result[1]
until cursor == "0"
