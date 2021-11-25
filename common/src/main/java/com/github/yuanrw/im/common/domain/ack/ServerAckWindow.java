package com.github.yuanrw.im.common.domain.ack;

import com.github.yuanrw.im.common.domain.ResponseCollector;
import com.github.yuanrw.im.common.exception.ImException;
import com.github.yuanrw.im.protobuf.generate.Internal;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 发送方窗口:
 *  维护ACK等待队列
 * Date: 2019-09-08
 * Time: 12:36
 *
 * @author yrw
 */
public class ServerAckWindow {
    private static Logger logger = LoggerFactory.getLogger(ServerAckWindow.class);
    /**
     * 分两种情况:
     * 1.服务端发送窗口，windowMap中存在多组{connection:ServerAckWindow}
     * 2.客户端发送窗口，windowMap中只有一组{connection:ServerAckWindow}
     */
    private static Map<Serializable, ServerAckWindow> windowsMap;
    /**
     * 发送方ACK等待队列轮询线程
     */
    private static ExecutorService executorService;

    private final Duration timeout;
    private final int maxSize;

    /**
     * ACK等待队列
     */
    private ConcurrentHashMap<Long, ResponseCollector<Internal.InternalMsg>> responseCollectorMap;

    static {
        windowsMap = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(ServerAckWindow::checkTimeoutAndRetry);
    }

    public ServerAckWindow(Serializable connectionId, int maxSize, Duration timeout) {
        this.responseCollectorMap = new ConcurrentHashMap<>();
        this.timeout = timeout;
        this.maxSize = maxSize;

        windowsMap.put(connectionId, this);
    }

    /**
     * multi thread do it
     *
     * @param id           msg id
     * @param sendMessage
     * @param sendFunction
     * @return
     */
    public static CompletableFuture<Internal.InternalMsg> offer(Serializable connectionId, Long id, Message sendMessage, Consumer<Message> sendFunction) {
        return windowsMap.get(connectionId).offer(id, sendMessage, sendFunction);
    }

    /**
     * multi thread do it
     *
     * @param id           msg id
     * @param sendMessage
     * @param sendFunction
     * @return
     */
    public CompletableFuture<Internal.InternalMsg> offer(Long id, Message sendMessage, Consumer<Message> sendFunction) {
        if (responseCollectorMap.containsKey(id)) {
            CompletableFuture<Internal.InternalMsg> future = new CompletableFuture<>();
            future.completeExceptionally(new ImException("send repeat msg id: " + id));
            return future;
        }
        if (responseCollectorMap.size() >= maxSize) {
            CompletableFuture<Internal.InternalMsg> future = new CompletableFuture<>();
            future.completeExceptionally(new ImException("server window is full"));
            return future;
        }

        ResponseCollector<Internal.InternalMsg> responseCollector = new ResponseCollector<>(sendMessage, sendFunction);
        responseCollector.send();
        responseCollectorMap.put(id, responseCollector);
        return responseCollector.getFuture();
    }

    public void ack(Internal.InternalMsg message) {
        Long id = Long.parseLong(message.getMsgBody());
        logger.debug("get ack, msg: {}", id);
        if (responseCollectorMap.containsKey(id)) {
            responseCollectorMap.get(id).getFuture().complete(message);
            responseCollectorMap.remove(id);
        }
    }

    /**
     * single thread do it
     * 轮询发送ACK等待队列，若有超时未收到ACK的，就取出消息重发
     *
     * 超时未收到ACK消息的两种处理方式:
     * 1.与TCP/IP一样不断发送直到收到ACK位置
     * 2.设置一个最大重试次数，超过这个次数还没收到ACK，就是用失败机制处理
     */
    private static void checkTimeoutAndRetry() {
        while (true) {
            for (ServerAckWindow window : windowsMap.values()) {
                window.responseCollectorMap.entrySet().stream()
                    .filter(entry -> window.timeout(entry.getValue()))
                    .forEach(entry -> window.retry(entry.getKey(), entry.getValue()));
            }
        }
    }

    private void retry(Long id, ResponseCollector<?> collector) {
        logger.debug("retry msg: {}", id);
        //todo: if offline
        collector.send();
    }

    private boolean timeout(ResponseCollector<?> collector) {
        return collector.getSendTime().get() != 0 && collector.timeElapse() > timeout.toNanos();
    }
}
