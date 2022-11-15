#!lua name=RTQ

redis.register_function('RTQ_PUSH', function(KEYS, ARGV)
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
        PREFIX:names - zset of NAME, also used as a template for `PREFIX:usage:WINDOW`
    ]]
    if #KEYS ~= 0 then
        error('RTQ_PUSH takes no key arguments!')
    end
    if #ARGV ~= 4 then
        error('RTQ_PUSH expected 4 arguments, but got ' .. #ARGV .. ' arguments!')
    end
    local PREFIX = ARGV[1]
    local NAME = ARGV[2]
    local PRIORITY = ARGV[3]
    local DATA = ARGV[4]

    local names_key = PREFIX .. ':names'
    local queue_key = PREFIX .. ':queue:' .. NAME
    local total_key = PREFIX .. ':total'
    local added = redis.call('ZADD', queue_key, 'GT', PRIORITY, DATA)
    if tonumber(added) > 0 then
        redis.call('INCR', total_key)
        redis.call('ZADD', names_key, 'NX', 0, NAME)
    end
end)

redis.register_function('RTQ_POP', function(KEYS, ARGV)
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
    if #KEYS ~= 0 then
        error('RTQ_POP takes no key arguments!')
    end
    if #ARGV ~= 4 then
        error('RTQ_POP expected 4 arguments, but got ' .. #ARGV .. ' arguments!')
    end
    local PREFIX = ARGV[1]
    local WINDOW = ARGV[2]
    local LIMIT = tonumber(ARGV[3])
    local RESOLUTION = ARGV[4]

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
end)

redis.register_function('RTQ_CLEANUP', function(KEYS, ARGV)
    --[[
    This script takes no KEYS arguments.

    ARGV arguments:
        PREFIX: Key prefix to cleanup.

    This will remove all keys matching PREFIX:*
    ]]
    if #KEYS ~= 0 then
        error('RTQ_CLEANUP takes no key arguments!')
    end
    if #ARGV ~= 1 then
        error('RTQ_CLEANUP expected 1 arguments, but got ' .. #ARGV .. ' arguments!')
    end
    local PREFIX = ARGV[1]
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
end)
