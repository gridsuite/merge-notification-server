/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.merge.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * A WebSocketHandler that sends messages from a broker to websockets opened by clients, interleaving with pings to keep connections open.
 *
 * Spring Cloud Stream gets the consumeNotification bean and calls it with the
 * flux from the broker. We call publish and connect to subscribe immediately to the flux
 * and multicast the messages to all connected websockets and to discard the messages when
 * no websockets are connected.
 *
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@Component
public class MergeNotificationWebSocketHandler implements WebSocketHandler {

    private static final String CATEGORY_BROKER_INPUT = MergeNotificationWebSocketHandler.class.getName() + ".input-broker-messages";
    private static final String CATEGORY_WS_OUTPUT = MergeNotificationWebSocketHandler.class.getName() + ".output-websocket-messages";

    private static final String QUERY_PROCESS = "process";
    private static final String QUERY_DATE = "date";

    private static final String HEADER_TSO = "tso";
    private static final String HEADER_TYPE = "type";
    private static final String HEADER_DATE = "date";
    private static final String HEADER_PROCESS = "process";

    private ObjectMapper jacksonObjectMapper;

    private int heartbeatInterval;

    public MergeNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
    }

    Flux<Message<String>> flux;

    @Bean
    public Consumer<Flux<Message<String>>> consumeNotification() {
        return f -> {
            ConnectableFlux<Message<String>> c = f.log(CATEGORY_BROKER_INPUT, Level.FINE).publish();
            this.flux = c;
            c.connect();
            // Force connect 1 fake subscriber to consumme messages as they come.
            // Otherwise, reactorcore buffers some messages (not until the connectable flux had
            // at least one subscriber. Is there a better way ?
            c.subscribe();
        };
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession, String process) {
        return flux.transform(f -> {
            Flux<Message<String>> res = f;
            if (process != null) {
                res = res.filter(m -> process.equals(m.getHeaders().get(HEADER_PROCESS)));
            }
            return res;
        }).map(m -> {
            try {
                Map<String, Object> submap = Map.of(
                        "payload", m.getPayload(),
                        "headers", Map.of(
                                HEADER_TSO, m.getHeaders().get(HEADER_TSO),
                                HEADER_TYPE, m.getHeaders().get(HEADER_TYPE),
                                HEADER_PROCESS, m.getHeaders().get(HEADER_PROCESS),
                                HEADER_DATE, m.getHeaders().get(HEADER_DATE)));
                return jacksonObjectMapper.writeValueAsString(submap);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }).map(webSocketSession::textMessage);
    }

    /**
     * A heartbeat flux sending websockets pings
     */
    private Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        URI uri = webSocketSession.getHandshakeInfo().getUri();
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
        String process = parameters.getFirst(QUERY_PROCESS);
        return webSocketSession
                .send(
                        notificationFlux(webSocketSession, process)
                        .mergeWith(heartbeatFlux(webSocketSession))
                        .log(CATEGORY_WS_OUTPUT, Level.FINE))
                .and(webSocketSession.receive());
    }
}
