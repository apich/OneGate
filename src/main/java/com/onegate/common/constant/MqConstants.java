package com.onegate.common.constant;

/**
 * RabbitMQ 交换机、队列与路由键常量定义。
 * <p>
 * 包含两条异步链路：
 * 1. 计费落库主链路 + 死信兜底链路 (Billing + DLX/DLQ)
 * 2. 预占冻结超时自动释放延迟链路 (Freeze TTL Delay -> Timeout Compensation)
 */
public final class MqConstants {

    private MqConstants() {
    }

    /** 计费主交换机 */
    public static final String BILLING_EXCHANGE = "onegate.billing.exchange";

    /** 计费主队列 (绑定 DLX) */
    public static final String BILLING_QUEUE = "onegate.billing.queue";

    /** 计费主路由键 */
    public static final String BILLING_ROUTING_KEY = "onegate.billing.record";

    /** 计费死信交换机 */
    public static final String BILLING_DLX = "onegate.billing.dlx";

    /** 计费死信队列 */
    public static final String BILLING_DLQ = "onegate.billing.dlq";

    /** 计费死信路由键 */
    public static final String BILLING_DLQ_ROUTING_KEY = "onegate.billing.dlq";

    /** 预占冻结延迟交换机 (无消费者, 消息依靠 TTL 过期路由至超时交换机) */
    public static final String FREEZE_DELAY_EXCHANGE = "onegate.freeze.delay.exchange";

    /** 预占冻结延迟队列 */
    public static final String FREEZE_DELAY_QUEUE = "onegate.freeze.delay.queue";

    /** 预占冻结延迟路由键 */
    public static final String FREEZE_DELAY_ROUTING_KEY = "onegate.freeze.delay";

    /** 预占冻结超时补偿交换机 (作为 FREEZE_DELAY_QUEUE 的 DLX) */
    public static final String FREEZE_TIMEOUT_EXCHANGE = "onegate.freeze.timeout.exchange";

    /** 预占冻结超时补偿队列 */
    public static final String FREEZE_TIMEOUT_QUEUE = "onegate.freeze.timeout.queue";

    /** 预占冻结超时补偿路由键 */
    public static final String FREEZE_TIMEOUT_ROUTING_KEY = "onegate.freeze.timeout";
}
