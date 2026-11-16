-- OneGate 预占额度核销与超时回滚互斥脚本 (Redis + Lua 原子执行)
--
-- KEYS[1]: 可用额度余额 Key -> onegate:quota:avail:{tenant_id}
-- KEYS[2]: 请求预占冻结 Key -> onegate:quota:freeze:{tenant_id}:{request_id}
--
-- ARGV[1]: actualCost 实际消耗 Token 数 (正常/断连核销传实际估算值; 超时/异常全额回滚传 0)
--
-- 核心互斥不变式:
--   先 GET freezeKey; 若为空说明已被正常核销或已被延迟超时回滚, 直接返回 {0, 0} (100% 幂等)。
--   否则原子 DEL freezeKey 并向 availKey 退还 (frozen - actualCost) 差额 (保持 seenKey 持续防重放)。
--   超额核销防负保护: 若 delta < 0 且 (avail + delta) < 0, 将实际补扣差额钳制为 -avail (最低扣至 0),
--   绝不允许 Redis 可用余额出现负值。
--
-- 返回数组:
--   {0, 0}                              -> freezeKey 不存在 (已核销或已回滚)
--   {1, appliedDelta, frozen, clamped}  -> 本次核销/回滚生效; clamped=1 表示发生透支截断

local availKey = KEYS[1]
local freezeKey = KEYS[2]

local actualCost = tonumber(ARGV[1])
if actualCost == nil or actualCost < 0 then
    actualCost = 0
end

local frozenStr = redis.call('GET', freezeKey)
if not frozenStr then
    return {0, 0}
end

local frozen = tonumber(frozenStr) or 0
redis.call('DEL', freezeKey)

local delta = frozen - actualCost
local appliedDelta = 0
local clamped = 0

local availStr = redis.call('GET', availKey)
if availStr then
    local avail = tonumber(availStr) or 0
    if delta < 0 and (avail + delta) < 0 then
        appliedDelta = avail > 0 and -avail or 0
        clamped = 1
    else
        appliedDelta = delta
    end
    if appliedDelta ~= 0 then
        redis.call('INCRBY', availKey, appliedDelta)
    end
end

return {1, appliedDelta, frozen, clamped}
