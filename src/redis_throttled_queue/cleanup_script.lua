--[[
This script takes no KEYS arguments.

ARGV arguments:
    PREFIX: Key prefix to cleanup.

This will remove all keys matching PREFIX:*
]]
if #ARGV ~= 1 then
    error('CLEANUP_SCRIPT expected 1 arguments, but got ' .. #ARGV .. ' arguments!')
end
local PREFIX = unpack(ARGV)
local pattern = PREFIX .. ':*'

local cursor = '0'
repeat
    local result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 1000)
    local keys = result[2]
    if #keys ~= 0 then
        redis.call('DEL', unpack(keys))
    end
    cursor = result[1]
until cursor == '0'
