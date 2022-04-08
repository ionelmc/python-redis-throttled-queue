--[[
This script takes no KEYS arguments.

ARGV arguments:
    PREFIX: Key prefix to use for any generated key.
    NAME: Name of the queue.
    PRIORITY: Priority of the item.
    DATA: Item value.

Typical key structure (for every ITEM returned M is incremented, but only while M < limit):

    PREFIX:queue:NAME - zset of ITEM
    PREFIX:total - int of total ITEM count
    PREFIX:names - set of NAME
]]
if #ARGV ~= 4 then
    error("PUSH_ITEM_SCRIPT expected 4 arguments, but got " .. #ARGV .. " arguments!")
end
local PREFIX, NAME, PRIORITY, DATA = unpack(ARGV)
local queue_key = PREFIX .. ":queue:" .. NAME
local total_key = PREFIX .. ":total"
local name_set_key = PREFIX .. ":names"
local added = redis.call('ZADD', queue_key, "GT", PRIORITY, DATA)
if tonumber(added) > 0 then
    redis.call('INCR', total_key)
    redis.call('SADD', name_set_key, NAME)
end
