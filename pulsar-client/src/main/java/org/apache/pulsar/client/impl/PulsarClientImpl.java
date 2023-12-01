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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.client.impl.transaction.TransactionBuilderImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClientImpl implements PulsarClient {

    private static final Logger log = LoggerFactory.getLogger(PulsarClientImpl.class);

    private final ClientConfigurationData conf;
    private LookupService lookup;
    private final ConnectionPool cnxPool;
    private final Timer timer;
    private boolean needStopTimer;
    private final ExecutorProvider externalExecutorProvider;
    private final ExecutorProvider internalExecutorService;

    public enum State {
        Open, Closing, Closed
    }

    private AtomicReference<State> state = new AtomicReference<>();
    private final Set<ProducerBase<?>> producers;
    private final Set<ConsumerBase<?>> consumers;

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator = new AtomicLong();

    private final EventLoopGroup eventLoopGroup;

    private final LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache = CacheBuilder.newBuilder().maximumSize(100000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, SchemaInfoProvider>() {

                @Override
                public SchemaInfoProvider load(String topicName) {
                    return newSchemaProvider(topicName);
                }
            });

    private final Clock clientClock;

    @Getter
    private TransactionCoordinatorClientImpl tcClient;

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, getEventLoopGroup(conf));
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, new ConnectionPool(conf, eventLoopGroup), null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool, Timer timer)
            throws PulsarClientException {
        if (conf == null || isBlank(conf.getServiceUrl()) || eventLoopGroup == null) {
            throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
        }
        this.eventLoopGroup = eventLoopGroup;
        setAuth(conf);
        this.conf = conf;
        this.clientClock = conf.getClock();
        conf.getAuthentication().start();
        this.cnxPool = cnxPool;
        externalExecutorProvider = new ExecutorProvider(conf.getNumListenerThreads(), getThreadFactory("pulsar-external-listener"));
        internalExecutorService = new ExecutorProvider(conf.getNumIoThreads(), getThreadFactory("pulsar-client-internal"));
        if (conf.getServiceUrl().startsWith("http")) {
            lookup = new HttpLookupService(conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(), conf.isUseTls(), externalExecutorProvider.getExecutor());
        }
        if (timer == null) {
            this.timer = new HashedWheelTimer(getThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
            needStopTimer = true;
        } else {
            this.timer = timer;
        }
        producers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        consumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

        if (conf.isEnableTransaction()) {
            tcClient = new TransactionCoordinatorClientImpl(this);
            try {
                tcClient.start();
            } catch (Throwable e) {
                log.error("Start transactionCoordinatorClient error.", e);
                throw new PulsarClientException(e);
            }
        }

        state.set(State.Open);
    }

    private void setAuth(ClientConfigurationData conf) throws PulsarClientException {
        if (StringUtils.isBlank(conf.getAuthPluginClassName())
                || (StringUtils.isBlank(conf.getAuthParams()) && conf.getAuthParamMap() == null)) {
            return;
        }

        if (StringUtils.isNotBlank(conf.getAuthParams())) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParams()));
        } else if (conf.getAuthParamMap() != null) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParamMap()));
        }
    }

    public ClientConfigurationData getConfiguration() {
        return conf;
    }

    @VisibleForTesting
    public Clock getClientClock() {
        return clientClock;
    }

    public AtomicReference<State> getState() {
        return state;
    }

    //创建生产者构造者实例
    @Override
    public ProducerBuilder<byte[]> newProducer() {
        return new ProducerBuilderImpl<>(this, Schema.BYTES);
    }

    //切换 ProducerBuilderImpl<T> 类，构造方法
    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        return new ProducerBuilderImpl<>(this, schema);
    }

    @Override
    public ConsumerBuilder<byte[]> newConsumer() {
        return new ConsumerBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return new ConsumerBuilderImpl<>(this, schema);
    }

    @Override
    public ReaderBuilder<byte[]> newReader() {
        return new ReaderBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
        return new ReaderBuilderImpl<>(this, schema);
    }

    public CompletableFuture<Producer<byte[]>> createProducerAsync(ProducerConfigurationData conf) {
        return createProducerAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf,  Schema<T> schema) {
        return createProducerAsync(conf, schema, null);
    }

    //异步创建生产者实例
    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf, Schema<T> schema,
          ProducerInterceptors interceptors) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }

        //如果Schema为自动消费者Schema实例，由于这里是生产者，故抛异常
        if (schema instanceof AutoConsumeSchema) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("AutoConsumeSchema is only used by consumers to detect schemas automatically"));
        }

        //Pulsar 客户端是否初始化完成，如果状态不是Open，则抛异常：客户端已关闭。
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed : state = " + state.get()));
        }

        String topic = conf.getTopicName();

        //对Topic进行格式校验
        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        //自动获取Schema
        if (schema instanceof AutoProduceBytesSchema) {
            AutoProduceBytesSchema autoProduceBytesSchema = (AutoProduceBytesSchema) schema;
            if (autoProduceBytesSchema.schemaInitialized()) {
                return createProducerAsync(topic, conf, schema, interceptors);
            }
            //这里就是查找服务，通过Topic查询注册的Schema，如果没注册，则默认Schema.BYTES
            return lookup.getSchema(TopicName.get(conf.getTopicName()))
                    .thenCompose(schemaInfoOptional -> {
                        if (schemaInfoOptional.isPresent()) {
                            autoProduceBytesSchema.setSchema(Schema.getSchema(schemaInfoOptional.get()));
                        } else {
                            autoProduceBytesSchema.setSchema(Schema.BYTES);
                        }
                        return createProducerAsync(topic, conf, schema, interceptors);
                    });
        } else {
            return createProducerAsync(topic, conf, schema, interceptors);
        }

    }

    private <T> CompletableFuture<Producer<T>> createProducerAsync(String topic,
                                                                   ProducerConfigurationData conf,
                                                                   Schema<T> schema,
                                                                   ProducerInterceptors interceptors) {
        CompletableFuture<Producer<T>> producerCreatedFuture = new CompletableFuture<>();

        //通过查找服务获取Topic元数据，这里主要是确定Topic是否分区，以此来确定用哪个底层生产者实例
        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ProducerBase<T> producer;
            if (metadata.partitions > 0) {
                producer = new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, metadata.partitions,
                        producerCreatedFuture, schema, interceptors);
            } else {
                producer = new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, -1, schema, interceptors);
            }

            producers.add(producer);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
            producerCreatedFuture.completeExceptionally(ex);
            return null;
        });

        return producerCreatedFuture;
    }

    public CompletableFuture<Consumer<byte[]>> subscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return subscribeAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Consumer<T>> subscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        // 检查 Client 状态是否打开
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        //检查 Topic 是否合法
        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
        }

        //订阅名不能为空
        if (isBlank(conf.getSubscriptionName())) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }

        //读取压缩特性仅仅被用订阅持久化 Topic，并且订阅类型为独占或故障转移模式
        if (conf.isReadCompacted() && (!conf.getTopicNames().stream()
                .allMatch(topic -> TopicName.get(topic).getDomain() == TopicDomain.persistent)
                || (conf.getSubscriptionType() != SubscriptionType.Exclusive
                        && conf.getSubscriptionType() != SubscriptionType.Failover))) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Read compacted can only be used with exclusive or failover persistent subscriptions"));
        }

        // 事件监听器只允许在故障转移订阅模式下
        if (conf.getConsumerEventListener() != null && conf.getSubscriptionType() != SubscriptionType.Failover) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Active consumer listener is only supported for failover subscription"));
        }

        //   如果已经设置了 正则表达式订阅 Topic，就不能指定具体的 Topic
        if (conf.getTopicsPattern() != null) {
            // If use topicsPattern, we should not use topic(), and topics() method.
            if (!conf.getTopicNames().isEmpty()){
                return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic names list must be null when use topicsPattern"));
            }
            //正则表达式订阅
            return patternTopicSubscribeAsync(conf, schema, interceptors);
        } else if (conf.getTopicNames().size() == 1) {
            //单 Topic 订阅
            return singleTopicSubscribeAsync(conf, schema, interceptors);
        } else {
            //多 Topic 订阅
            return multiTopicSubscribeAsync(conf, schema, interceptors);
        }
    }

    private <T> CompletableFuture<Consumer<T>> singleTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getSingleTopic())
            .thenCompose(schemaClone -> doSingleTopicSubscribeAsync(conf, schemaClone, interceptors));
    }

    private <T> CompletableFuture<Consumer<T>> doSingleTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        String topic = conf.getSingleTopic();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                //这里注意，多分区 Topic 也对应着多 Topic 消费者实现 ？？？
                consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(PulsarClientImpl.this, conf,
                        externalExecutorProvider, consumerSubscribedFuture, metadata.partitions, schema, interceptors);
            } else {
                //单 Topic 消费者实现
                int partitionIndex = TopicName.getPartitionIndex(topic);
                consumer = ConsumerImpl.newConsumerImpl(PulsarClientImpl.this, topic, conf, externalExecutorProvider,
                        partitionIndex, false, consumerSubscribedFuture, null, schema, interceptors,
                        true /* createTopicIfDoesNotExist */);
            }

            consumers.add(consumer);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            consumerSubscribedFuture.completeExceptionally(ex);
            return null;
        });

        return consumerSubscribedFuture;
    }

    // 多 Topic 订阅，这里是真正的多 Topic 订阅，但可能其中0或多个 Topic 是多分区
    private <T> CompletableFuture<Consumer<T>> multiTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        ConsumerBase<T> consumer = new MultiTopicsConsumerImpl<>(PulsarClientImpl.this, conf,
                externalExecutorProvider, consumerSubscribedFuture, schema, interceptors,
                true /* createTopicIfDoesNotExist */);

        consumers.add(consumer);

        return consumerSubscribedFuture;
    }

    public CompletableFuture<Consumer<byte[]>> patternTopicSubscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return patternTopicSubscribeAsync(conf, Schema.BYTES, null);
    }

    //Topic 正则表达式订阅
    private <T> CompletableFuture<Consumer<T>> patternTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
            Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        String regex = conf.getTopicsPattern().pattern();
        Mode subscriptionMode = convertRegexSubscriptionMode(conf.getRegexSubscriptionMode());
        TopicName destination = TopicName.get(regex);
        //有正则表达式解析出 Namespace ，例如：persistent://public/default/.*
        NamespaceName namespaceName = destination.getNamespaceObject();

        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        //根据 Namespace 和 订阅模式来查找所有符合条件的 Topic
        lookup.getTopicsUnderNamespace(namespaceName, subscriptionMode)
            .thenAccept(topics -> {
                if (log.isDebugEnabled()) {
                    log.debug("Get topics under namespace {}, topics.size: {}", namespaceName.toString(), topics.size());
                    topics.forEach(topicName ->
                        log.debug("Get topics under namespace {}, topic: {}", namespaceName.toString(), topicName));
                }

                //这里进行正则表达式过滤
                List<String> topicsList = topicsPatternFilter(topics, conf.getTopicsPattern());
                //配置好新的 Topic
                conf.getTopicNames().addAll(topicsList);
                ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<T>(conf.getTopicsPattern(),
                    PulsarClientImpl.this,
                    conf,
                    externalExecutorProvider,
                    consumerSubscribedFuture,
                    schema, subscriptionMode, interceptors);

                consumers.add(consumer);
            })
            .exceptionally(ex -> {
                log.warn("[{}] Failed to get topics under namespace", namespaceName);
                consumerSubscribedFuture.completeExceptionally(ex);
                return null;
            });

        return consumerSubscribedFuture;
    }

    // 从原Topic列表中匹配 ‘Topic正则表达式’，结果返回仅仅是 Topic 名，无分区部分
    // get topics that match 'topicsPattern' from original topics list
    // return result should contain only topic names, without partition part
    public static List<String> topicsPatternFilter(List<String> original, Pattern topicsPattern) {
        final Pattern shortenedTopicsPattern = topicsPattern.toString().contains("://")
            ? Pattern.compile(topicsPattern.toString().split("\\:\\/\\/")[1]) : topicsPattern;

        return original.stream()
            .map(TopicName::get)
            .map(TopicName::toString)
            .filter(topic -> shortenedTopicsPattern.matcher(topic.split("\\:\\/\\/")[1]).matches())
            .collect(Collectors.toList());
    }

    public CompletableFuture<Reader<byte[]>> createReaderAsync(ReaderConfigurationData<byte[]> conf) {
        return createReaderAsync(conf, Schema.BYTES);
    }

    public <T> CompletableFuture<Reader<T>> createReaderAsync(ReaderConfigurationData<T> conf, Schema<T> schema) {
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getTopicName())
            .thenCompose(schemaClone -> doCreateReaderAsync(conf, schemaClone));
    }

    <T> CompletableFuture<Reader<T>> doCreateReaderAsync(ReaderConfigurationData<T> conf, Schema<T> schema) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        String topic = conf.getTopicName();

        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }

        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }
            if (metadata.partitions > 0 && MultiTopicsConsumerImpl.isIllegalMultiTopicsMessageId(conf.getStartMessageId())) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("The partitioned topic startMessageId is illegal"));
                return;
            }
            CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
            Reader<T> reader;
            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                reader = new MultiTopicsReaderImpl<>(PulsarClientImpl.this,
                        conf, externalExecutorProvider, consumerSubscribedFuture, schema);
                consumer = ((MultiTopicsReaderImpl<T>) reader).getMultiTopicsConsumer();
            } else {
                reader = new ReaderImpl<>(PulsarClientImpl.this, conf, externalExecutorProvider, consumerSubscribedFuture, schema);
                consumer = ((ReaderImpl<T>) reader).getConsumer();
            }

            consumers.add(consumer);

            consumerSubscribedFuture.thenRun(() -> {
                readerFuture.complete(reader);
            }).exceptionally(ex -> {
                log.warn("[{}] Failed to get create topic reader", topic, ex);
                readerFuture.completeExceptionally(ex);
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            readerFuture.completeExceptionally(ex);
            return null;
        });

        return readerFuture;
    }

    /**
     * Read the schema information for a given topic.
     *
     * If the topic does not exist or it has no schema associated, it will return an empty response
     */
    public CompletableFuture<Optional<SchemaInfo>> getSchema(String topic) {
        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Throwable t) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        return lookup.getSchema(topicName);
    }

    //同步关闭，实际上就是调用异步关闭方法，等待完成
    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            PulsarClientException unwrapped = PulsarClientException.unwrap(e);
            if (unwrapped instanceof PulsarClientException.AlreadyClosedException) {
                // this is not a problem
                return;
            }
            throw unwrapped;
        }
    }

    //异步关闭
    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info("Client closing. URL: {}", lookup.getServiceUrl());
        //校验状态，如果不是Open状态，抛异常
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        //依次（异步）关闭生产者、消费者
        producers.forEach(p -> futures.add(p.closeAsync()));
        consumers.forEach(c -> futures.add(c.closeAsync()));

        // Need to run the shutdown sequence in a separate thread to prevent deadlocks
        // If there are consumers or producers that need to be shutdown we cannot use the same thread
        // to shutdown the EventLoopGroup as well as that would be trying to shutdown itself thus a deadlock
        // would happen
        //等待所有的生产者消费者正常关闭，这样就优雅的关闭客户端了
        FutureUtil.waitForAll(futures).thenRun(() -> new Thread(() -> {
            // All producers & consumers are now closed, we can stop the client safely
            try {
                shutdown();
                closeFuture.complete(null);
                state.set(State.Closed);
            } catch (PulsarClientException e) {
                closeFuture.completeExceptionally(e);
            }
        }, "pulsar-client-shutdown-thread").start()).exceptionally(exception -> {
            closeFuture.completeExceptionally(exception);
            return null;
        });

        return closeFuture;
    }

    @Override
    public void shutdown() throws PulsarClientException {
        try {
            //关闭查找
            lookup.close();
            //关闭连接池
            cnxPool.close();
            //定时器关闭
            if (needStopTimer) {
                timer.stop();
            }
            //额外执行线程池关闭
            externalExecutorProvider.shutdownNow();
            internalExecutorService.shutdownNow();
            //认证服务关闭
            conf.getAuthentication().close();
        } catch (Throwable t) {
            log.warn("Failed to shutdown Pulsar client", t);
            throw PulsarClientException.unwrap(t);
        }
    }

    @Override
    public boolean isClosed() {
        State currentState = state.get();
        return currentState == State.Closed || currentState == State.Closing;
    }

    @Override
    public synchronized void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        log.info("Updating service URL to {}", serviceUrl);

        conf.setServiceUrl(serviceUrl);
        lookup.updateServiceUrl(serviceUrl);
        cnxPool.closeAllConnections();
    }

    protected CompletableFuture<ClientCnx> getConnection(final String topic) {
        //解析Topic
        TopicName topicName = TopicName.get(topic);

        //从查找服务里读取存活的broker 地址，异步返回 ClientCnx ，根据成功与失败回调 Connection 接口中2个方法。
        return lookup.getBroker(topicName)
                .thenCompose(pair -> cnxPool.getConnection(pair.getLeft(), pair.getRight()));
    }

    /** visible for pulsar-functions **/
    public Timer timer() {
        return timer;
    }

    public ExecutorProvider externalExecutorProvider() {
        return externalExecutorProvider;
    }

    long newProducerId() {
        return producerIdGenerator.getAndIncrement();
    }

    long newConsumerId() {
        return consumerIdGenerator.getAndIncrement();
    }

    public long newRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    public ConnectionPool getCnxPool() {
        return cnxPool;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public LookupService getLookup() {
        return lookup;
    }

    public void reloadLookUp() throws PulsarClientException {
        if (conf.getServiceUrl().startsWith("http")) {
            lookup = new HttpLookupService(conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(), conf.isUseTls(),
                    externalExecutorProvider.getExecutor());
        }
    }

    public CompletableFuture<Integer> getNumberOfPartitions(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> metadata.partitions);
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(String topic) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();

        try {
            TopicName topicName = TopicName.get(topic);
            AtomicLong opTimeoutMs = new AtomicLong(conf.getOperationTimeoutMs());
            Backoff backoff = new BackoffBuilder()
                    .setInitialTime(100, TimeUnit.MILLISECONDS)
                    .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                    .setMax(1, TimeUnit.MINUTES)
                    .create();
            getPartitionedTopicMetadata(topicName, backoff, opTimeoutMs, metadataFuture);
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private void getPartitionedTopicMetadata(TopicName topicName,
                                             Backoff backoff,
                                             AtomicLong remainingTime,
                                             CompletableFuture<PartitionedTopicMetadata> future) {
        lookup.getPartitionedTopicMetadata(topicName).thenAccept(future::complete).exceptionally(e -> {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            // skip retry scheduler when set lookup throttle in client or server side which will lead to `TooManyRequestsException`
            boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
                || e.getCause() instanceof PulsarClientException.TooManyRequestsException
                || e.getCause() instanceof PulsarClientException.AuthenticationException;
            if (nextDelay <= 0 || isLookupThrottling) {
                future.completeExceptionally(e);
                return null;
            }

            ((ScheduledExecutorService) externalExecutorProvider.getExecutor()).schedule(() -> {
                log.warn("[topic: {}] Could not get connection while getPartitionedTopicMetadata -- Will try again in {} ms",
                    topicName, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getPartitionedTopicMetadata(topicName, backoff, remainingTime, future);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> {
            if (metadata.partitions > 0) {
                TopicName topicName = TopicName.get(topic);
                List<String> partitions = new ArrayList<>(metadata.partitions);
                for (int i = 0; i < metadata.partitions; i++) {
                    partitions.add(topicName.getPartition(i).toString());
                }
                return partitions;
            } else {
                return Collections.singletonList(topic);
            }
        });
    }

    private static EventLoopGroup getEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = getThreadFactory("pulsar-client-io");
        return EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);
    }

    private static ThreadFactory getThreadFactory(String poolName) {
        return new DefaultThreadFactory(poolName, Thread.currentThread().isDaemon());
    }

    void cleanupProducer(ProducerBase<?> producer) {
        synchronized (producers) {
            producers.remove(producer);
        }
    }

    void cleanupConsumer(ConsumerBase<?> consumer) {
        synchronized (consumers) {
            consumers.remove(consumer);
        }
    }

    @VisibleForTesting
    int producersCount() {
        synchronized (producers) {
            return producers.size();
        }
    }

    @VisibleForTesting
    int consumersCount() {
        synchronized (consumers) {
            return consumers.size();
        }
    }

    private static Mode convertRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
        switch (regexSubscriptionMode) {
        case PersistentOnly:
            return Mode.PERSISTENT;
        case NonPersistentOnly:
            return Mode.NON_PERSISTENT;
        case AllTopics:
            return Mode.ALL;
        default:
            return null;
        }
    }

    private SchemaInfoProvider newSchemaProvider(String topicName) {
        return new MultiVersionSchemaInfoProvider(TopicName.get(topicName), this);
    }

    private LoadingCache<String, SchemaInfoProvider> getSchemaProviderLoadingCache() {
        return schemaProviderLoadingCache;
    }

    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<Schema<T>> preProcessSchemaBeforeSubscribe(PulsarClientImpl pulsarClientImpl,
                                                                      Schema<T> schema,
                                                                      String topicName) {
        if (schema != null && schema.supportSchemaVersioning()) {
            final SchemaInfoProvider schemaInfoProvider;
            try {
                schemaInfoProvider = pulsarClientImpl.getSchemaProviderLoadingCache().get(topicName);
            } catch (ExecutionException e) {
                log.error("Failed to load schema info provider for topic {}", topicName, e);
                return FutureUtil.failedFuture(e.getCause());
            }
            schema = schema.clone();
            if (schema.requireFetchingSchemaInfo()) {
                Schema finalSchema = schema;
                return schemaInfoProvider.getLatestSchema().thenCompose(schemaInfo -> {
                    if (null == schemaInfo) {
                        if (!(finalSchema instanceof AutoConsumeSchema)) {
                            // no schema info is found
                            return FutureUtil.failedFuture(
                                    new PulsarClientException.NotFoundException(
                                            "No latest schema found for topic " + topicName));
                        }
                    }
                    try {
                        log.info("Configuring schema for topic {} : {}", topicName, schemaInfo);
                        finalSchema.configureSchemaInfo(topicName, "topic", schemaInfo);
                    } catch (RuntimeException re) {
                        return FutureUtil.failedFuture(re);
                    }
                    finalSchema.setSchemaInfoProvider(schemaInfoProvider);
                    return CompletableFuture.completedFuture(finalSchema);
                });
            } else {
                schema.setSchemaInfoProvider(schemaInfoProvider);
            }
        }
        return CompletableFuture.completedFuture(schema);
    }

    public ExecutorService getInternalExecutorService() {
        return internalExecutorService.getExecutor();
    }
    //
    // Transaction related API
    //

    // This method should be exposed in the PulsarClient interface. Only expose it when all the transaction features
    // are completed.
    // @Override
    public TransactionBuilder newTransaction() {
        return new TransactionBuilderImpl(this, tcClient);
    }

}
