-- OneGate 请求级额度预占冻结脚本 (Redis + Lua 原子执行)
--
-- KEYS[1]: 可用额度余额 Key       -> onegate:quota:avail:{tenant_id}
-- KEYS[2]: 请求预占冻结 Key       -> onegate:quota:freeze:{tenant_id}:{request_id}
-- KEYS[3]: 请求已受理防重放 Key   -> onegate:quota:seen:{tenant_id}:{request_id}
--
-- ARGV[1]: estCost          本次请求预估冻结 Token 数 (Prompt + max_tokens, >= 0)
-- ARGV[2]: freezeTtlSeconds 预占冻结 Key TTL (秒), 防止客户端或节点异常导致死账
-- ARGV[3]: seenTtlSeconds   请求防重放幂等标记 TTL (秒), 默认 86400
--
-- 返回数组:
--   {1, newAvail}  -> 预占冻结成功, 返回剩余可用额度
--   {0, avail}     -> 可用额度不足 (触发 HTTP 402 INSUFFICIENT_QUOTA)
--   {-1, 0}        -> availKey 尚未加载, 需 Java 层加 Redisson 锁回源 MySQL 并 NX 初始化后重试
--   {-2, avail}    -> 同一 request_id 正在冻结或已受理重放 (重复请求)
--   {-3, 0}        -> 非法参数

local availKey = KEYS[1]
local freezeKey = KEYS[2]
local seenKey = KEYS[3]

local estCost = tonumber(ARGV[1])
local ttlSeconds = tonumber(ARGV[2])
local seenTtlSeconds = tonumber(ARGV[3])

if estCost == nil or estCost < 0 or ttlSeconds == nil or ttlSeconds <= 0
        or seenTtlSeconds == nil or seenTtlSeconds <= 0 then
    return {-3, 0}
end

local availStr = redis.call('GET', availKey)
if not availStr then
    return {-1, 0}
end

local avail = tonumber(availStr)
if avail == nil then
    return {-1, 0}
end

if redis.call('EXISTS', freezeKey) == 1 or redis.call('EXISTS', seenKey) == 1 then
    return {-2, avail}
end

if avail < estCost then
    return {0, avail}
end

local newAvail = avail
if estCost > 0 then
    newAvail = redis.call('DECRBY', availKey, estCost)
end
redis.call('SET', freezeKey, tostring(estCost), 'EX', ttlSeconds)
redis.call('SET', seenKey, '1', 'EX', seenTtlSeconds)

return {1, newAvail}
