/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.mqtt.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.strimzi.kafka.bridge.mqtt.MqttSslContextProvider;
import io.strimzi.kafka.bridge.mqtt.config.BridgeConfig;
import io.strimzi.kafka.bridge.mqtt.kafka.KafkaBridgeProducer;

/**
 * This helper class help us add necessary Netty pipelines handlers. <br>
 * During the {@link #initChannel(Channel)}, we use MqttDecoder() and MqttEncoder to decode and encode Mqtt messages respectively. <br>
 */
public class MqttServerInitializer extends ChannelInitializer<Channel> {
    private final MqttServerHandler mqttServerHandler;
    private final int decoderMaxBytesInMessage;
    private final MqttSslContextProvider sslContextProvider;

    /**
     * Constructor
     *
     * @param kafkaBridgeProducer instance of the Kafka producer for sending messages
     * @param bridgeConfig        bridge configuration properties
     * @param sslContextProvider  SSL context provider
     */
    public MqttServerInitializer(KafkaBridgeProducer kafkaBridgeProducer, BridgeConfig bridgeConfig, MqttSslContextProvider sslContextProvider) {
        this.mqttServerHandler = new MqttServerHandler(kafkaBridgeProducer, bridgeConfig.getBridgeDefaultTopic());
        this.decoderMaxBytesInMessage = bridgeConfig.getMqttConfig().getMaxBytesMessage();
        this.sslContextProvider = sslContextProvider;
    }

    @Override
    protected void initChannel(Channel ch) {
        if (sslContextProvider != null) {
            ch.pipeline().addLast("ssl", sslContextProvider.newHandler(ch.alloc()));
        }

        ch.pipeline().addLast("decoder", new MqttDecoder(decoderMaxBytesInMessage));
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler", this.mqttServerHandler);
    }
}
