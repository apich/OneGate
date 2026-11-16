-- OneGate 租户级双指标 (RPM 滑动窗口 + TPM 令牌桶) 单次原子限流脚本
--
-- 设计原则:
-- 1. 必须配合 StringRedisTemplate 使用, 避免 Jackson JSON 序列化为字符串附加双引号,
--    否则 Lua tonumber() 对带引号值(如 "\"100\"") 会返回 nil。
-- 2. KEYS[1] 与 KEYS[2] 均使用 {tenant_id} HashTag, 在 Redis Cluster 中保证同一槽位单节点原子执行。
-- 3. 原子不变式: 仅当 RPM 与 TPM 双重校验均通过时, 才提交 ZADD(RPM) 与 HSET(TPM) 状态变更;
--    任意维度超限直接拒绝, 绝不污染另一维度的计数或 last_ts。
--
-- KEYS[1]: RPM 滑动窗口 ZSet Key -> onegate:rl:rpm:{tenantId}
-- KEYS[2]: TPM 令牌桶 Hash Key   -> onegate:rl:tpm:{tenantId}
--
-- ARGV[1]: nowMs           当前时间戳(毫秒)
-- ARGV[2]: rpmLimit        租户 RPM 上限 (每分钟最大请求数)
-- ARGV[3]: tpmLimit        租户 TPM 上限 (每分钟最大 Token 数, 兼作桶容量上限)
-- ARGV[4]: requestedTokens 本次请求预估消耗 Token 数 (>= 0)
-- ARGV[5]: rpmMember       ZSet member (由 requestId + 随机 nonce 组成, 避免相同 request_id 覆盖滑动窗口计数)
-- ARGV[6]: windowMs        滑动窗口长度(毫秒), 通常为 60000
-- ARGV[7]: expireMs        限流 Key 空闲过期时间(毫秒), 通常为 65000
--
-- 返回数组:
--   放行: {1, 0, newRpm, math.floor(newTokens)}
--   拒绝: {0, rejectCode(1=RPM超限, 2=TPM超限), currentRpm, math.floor(currentTokens)}

local rpmKey = KEYS[1]
local tpmKey = KEYS[2]

local nowMs = tonumber(ARGV[1])
local rpmLimit = tonumber(ARGV[2])
local tpmLimit = tonumber(ARGV[3])
local requestedTokens = tonumber(ARGV[4])
local rpmMember = ARGV[5]
local windowMs = tonumber(ARGV[6])
local expireMs = tonumber(ARGV[7])

if nowMs == nil or rpmLimit == nil or tpmLimit == nil or requestedTokens == nil or requestedTokens < 0
        or rpmMember == nil or rpmMember == '' or windowMs == nil or windowMs <= 0 or expireMs == nil or expireMs <= 0 then
    return redis.error_reply('ERR invalid rate limit arguments')
end

-- 1. 清理 RPM 滑动窗口外 [0, nowMs - windowMs] 的过期请求记录
local windowStart = nowMs - windowMs
redis.call('ZREMRANGEBYSCORE', rpmKey, '-inf', windowStart)
local currentRpm = tonumber(redis.call('ZCARD', rpmKey))

-- 2. 读取并仅在内存中模拟计算 TPM 令牌桶当前可用令牌数 (暂不写入 Redis)
local bucket = redis.call('HMGET', tpmKey, 'tokens', 'last_ts')
local storedTokens = tonumber(bucket[1])
local lastTs = tonumber(bucket[2])

local currentTokens
local effectiveLastTs
if storedTokens == nil or lastTs == nil then
    -- 首次初始化或桶已过期: 以满桶 tpmLimit 起步
    currentTokens = tpmLimit
    effectiveLastTs = nowMs
else
    -- 若管理员动态下调过 tpmLimit, 先把历史余额钳制到新容量上限以内
    if storedTokens > tpmLimit then
        storedTokens = tpmLimit
    end
    local elapsedMs = nowMs - lastTs
    if elapsedMs > 0 then
        local refillRatePerMs = tpmLimit / windowMs
        local refilled = storedTokens + (elapsedMs * refillRatePerMs)
        if refilled > tpmLimit then
            currentTokens = tpmLimit
        else
            currentTokens = refilled
        end
        effectiveLastTs = nowMs
    else
        -- 时钟微小乱序防御: 保持 last_ts 不倒退, 避免回充倒扣
        currentTokens = storedTokens
        effectiveLastTs = lastTs
    end
end

-- 3. 双指标联合校验: 任一不满足直接拒绝, 绝不修改对方状态
if currentRpm >= rpmLimit then
    return {0, 1, currentRpm, math.floor(currentTokens)}
end

if currentTokens < requestedTokens then
    return {0, 2, currentRpm, math.floor(currentTokens)}
end

-- 4. 双方均满足: 原子写入 RPM 记录与 TPM 扣减结果 (保留小数精度避免低速率回充饿死)
-- ZSet member 采用 requestId + 随机 nonce 避免相同 ID 覆盖滑动窗口计数
redis.call('ZADD', rpmKey, nowMs, rpmMember)
redis.call('PEXPIRE', rpmKey, expireMs)

local newTokens = currentTokens - requestedTokens
redis.call('HSET', tpmKey,
        'tokens', string.format('%.6f', newTokens),
        'last_ts', tostring(effectiveLastTs))
redis.call('PEXPIRE', tpmKey, expireMs)

local newRpm = currentRpm + 1
return {1, 0, newRpm, math.floor(newTokens)}
