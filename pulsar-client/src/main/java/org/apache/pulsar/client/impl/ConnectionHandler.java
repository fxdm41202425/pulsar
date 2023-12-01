/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHandler {
    //用于原子更新ClientCnx
    private static final AtomicReferenceFieldUpdater<ConnectionHandler, ClientCnx> CLIENT_CNX_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ConnectionHandler.class, ClientCnx.class, "clientCnx");

    //封装底层的客户端控制上下文，是实际的处理网络连接的
    @SuppressWarnings("unused")
    private volatile ClientCnx clientCnx = null;

    //客户端句柄状态
    protected final HandlerState state;

    //用于自动重连的时间组件
    protected final Backoff backoff;
    private static final AtomicLongFieldUpdater<ConnectionHandler> EPOCH_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConnectionHandler.class, "epoch");
    private volatile long epoch = 0L;
    protected volatile long lastConnectionClosedTimestamp = 0L;

    //connection 用于回调
    interface Connection {
        void connectionFailed(PulsarClientException exception);
        void connectionOpened(ClientCnx cnx);
    }

    protected Connection connection;

    protected ConnectionHandler(HandlerState state, Backoff backoff, Connection connection) {
        this.state = state;
        this.connection = connection;
        this.backoff = backoff;
        CLIENT_CNX_UPDATER.set(this, null);
    }

    protected void grabCnx() {
        //如果ClientCnx不为空，则直接返回，因为已经设置过了，这时候就忽略重连请求
        if (CLIENT_CNX_UPDATER.get(this) != null) {
            log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request", state.topic, state.getHandlerName());
            return;
        }

        //判定下是否能重连，只有Uninitialized、Connecting、Ready才能重连
        if (!isValidStateForReconnection()) {
            // Ignore connection closed when we are shutting down
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
            return;
        }

        //这时候，客户端开始获取连接 ， 如果连接发送异常，则延后重连
        try {
            state.client.getConnection(state.topic) //
                    .thenAccept(cnx -> connection.connectionOpened(cnx)) //
                    .exceptionally(this::handleConnectionError);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Exception thrown while getting connection: ", state.topic, state.getHandlerName(), t);
            reconnectLater(t);
        }
    }

    ////否则，执行 HandelConnectionError 方法，但实际上同样是 Connection 接口的 connectionFailed 方法，如下
    private Void handleConnectionError(Throwable exception) {
        log.warn("[{}] [{}] Error connecting to broker: {}", state.topic, state.getHandlerName(), exception.getMessage());
        if (exception instanceof PulsarClientException) {
            connection.connectionFailed((PulsarClientException) exception);
        } else if (exception.getCause() instanceof  PulsarClientException) {
            connection.connectionFailed((PulsarClientException)exception.getCause());
        } else {
            connection.connectionFailed(new PulsarClientException(exception));
        }

        // 如果客户端状态为 State.Uninitialized 或 State.Connecting  或  State.Ready，
        // 则稍后重连
        State state = this.state.getState();
        if (state == State.Uninitialized || state == State.Connecting || state == State.Ready) {
            reconnectLater(exception);
        }

        return null;
    }

    protected void reconnectLater(Throwable exception) {
        CLIENT_CNX_UPDATER.set(this, null);
        if (!isValidStateForReconnection()) {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
            return;
        }
        long delayMs = backoff.next();
        log.warn("[{}] [{}] Could not get connection to broker: {} -- Will try again in {} s", state.topic, state.getHandlerName(),
                exception.getMessage(), delayMs / 1000.0);
        state.setState(State.Connecting);
        state.client.timer().newTimeout(timeout -> {
            log.info("[{}] [{}] Reconnecting after connection was closed", state.topic, state.getHandlerName());
            incrementEpoch();
            grabCnx();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    protected long incrementEpoch() {
        return EPOCH_UPDATER.incrementAndGet(this);
    }

    @VisibleForTesting
    public void connectionClosed(ClientCnx cnx) {
        lastConnectionClosedTimestamp = System.currentTimeMillis();
        state.client.getCnxPool().releaseConnection(cnx);
        if (CLIENT_CNX_UPDATER.compareAndSet(this, cnx, null)) {
            if (!isValidStateForReconnection()) {
                log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
                return;
            }
            long delayMs = backoff.next();
            state.setState(State.Connecting);
            log.info("[{}] [{}] Closed connection {} -- Will try again in {} s", state.topic, state.getHandlerName(), cnx.channel(),
                    delayMs / 1000.0);
            state.client.timer().newTimeout(timeout -> {
                log.info("[{}] [{}] Reconnecting after timeout", state.topic, state.getHandlerName());
                incrementEpoch();
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    protected void resetBackoff() {
        backoff.reset();
    }

    @VisibleForTesting
    public ClientCnx cnx() {
        return CLIENT_CNX_UPDATER.get(this);
    }

    protected void setClientCnx(ClientCnx clientCnx) {
        CLIENT_CNX_UPDATER.set(this, clientCnx);
    }

    private boolean isValidStateForReconnection() {
        State state = this.state.getState();
        switch (state) {
            case Uninitialized:
            case Connecting:
            case Ready:
                // Ok
                return true;

            case Closing:
            case Closed:
            case Failed:
            case Terminated:
                return false;
        }
        return false;
    }

    @VisibleForTesting
    public long getEpoch() {
        return epoch;
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);
}
