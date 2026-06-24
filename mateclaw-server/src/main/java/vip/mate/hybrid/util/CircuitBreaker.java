package vip.mate.hybrid.util;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 简易熔断器。
 * <p>
 * 三态切换：CLOSED（正常）→ OPEN（熔断）→ HALF_OPEN（探测）→ CLOSED/HALF_OPEN。
 * 用于 HybridRetriever 中 ES/Milvus 的故障隔离。
 * <p>
 * 默认配置：3 次连续失败触发熔断，60s 后进入半开探测。
 *
 * @author MateClaw Team
 */
@Slf4j
public class CircuitBreaker {

    public enum State {
        /** 正常状态：请求正常通过 */
        CLOSED,
        /** 熔断状态：拒绝请求，返回降级 */
        OPEN,
        /** 半开探测：允许少量请求通过，成功则恢复 */
        HALF_OPEN
    }

    private final String name;
    private final int failureThreshold;
    private final long resetTimeoutMs;

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private volatile LocalDateTime lastFailureTime;

    public CircuitBreaker(String name) {
        this(name, 3, 60_000);
    }

    public CircuitBreaker(String name, int failureThreshold, long resetTimeoutMs) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    /**
     * 判断当前是否应触发降级。
     *
     * @return true = 应降级（拒绝请求）
     */
    public boolean shouldFallback() {
        State currentState = state.get();

        if (currentState == State.OPEN) {
            if (shouldReset()) {
                state.compareAndSet(State.OPEN, State.HALF_OPEN);
                log.info("[CircuitBreaker] {} transitioning to HALF_OPEN for probe", name);
                return false;
            }
            log.debug("[CircuitBreaker] {} is OPEN, using fallback", name);
            return true;
        }

        return false;
    }

    /**
     * 记录一次成功调用。
     * HALF_OPEN → CLOSED，重置失败计数。
     */
    public void recordSuccess() {
        failureCount.set(0);
        state.compareAndSet(State.HALF_OPEN, State.CLOSED);
        log.debug("[CircuitBreaker] {} recorded success, state reset to CLOSED", name);
    }

    /**
     * 记录一次失败调用。
     * 累计达到阈值时 CLOSED → OPEN 或 HALF_OPEN → OPEN。
     */
    public void recordFailure() {
        int count = failureCount.incrementAndGet();
        lastFailureTime = LocalDateTime.now();

        if (count >= failureThreshold && state.compareAndSet(State.CLOSED, State.OPEN)) {
            log.warn("[CircuitBreaker] {} tripped after {} failures, state=OPEN", name, count);
        } else if (state.get() == State.HALF_OPEN) {
            state.compareAndSet(State.HALF_OPEN, State.OPEN);
            log.warn("[CircuitBreaker] {} probe failed, staying OPEN", name);
        }
    }

    public State getState() {
        return state.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    private boolean shouldReset() {
        if (lastFailureTime == null) {
            return true;
        }
        long elapsedMs = Duration.between(lastFailureTime, LocalDateTime.now()).toMillis();
        return elapsedMs >= resetTimeoutMs;
    }
}
