/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.merge.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 */
public class MergeNotificationWebSocketHandlerTest {

    private ObjectMapper objectMapper;
    private WebSocketSession ws;
    private HandshakeInfo handshakeinfo;
    private UriComponentsBuilder uriComponentBuilder;

    @Before
    public void setup() {
        objectMapper = new ObjectMapper();
        var dataBufferFactory = new DefaultDataBufferFactory();

        ws = Mockito.mock(WebSocketSession.class);
        handshakeinfo = Mockito.mock(HandshakeInfo.class);
        uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
        when(ws.getHandshakeInfo()).thenReturn(handshakeinfo);
        when(ws.receive()).thenReturn(Flux.empty());
        when(ws.send(any())).thenReturn(Mono.empty());
        when(ws.textMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String str = (String) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(str.getBytes()));
        });
        when(ws.pingMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Function<DataBufferFactory, DataBuffer> f = (Function<DataBufferFactory, DataBuffer>) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.PING, f.apply(dataBufferFactory));
        });
        when(ws.getId()).thenReturn("testsession");

    }

    private void withFilters(String process) {
        var notificationWebSocketHandler = new MergeNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);

        if (process != null) {
            uriComponentBuilder.queryParam("process", process);
        }

        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();

        notificationWebSocketHandler.handle(ws);

        List<Map<String, Object>> refMessages = Arrays.asList(
                Map.of("process", "SWE", "date", "2020-07-01 02:30:00.000000+0000", "tso", "RTE", "status", "TEST"),
                Map.of("process", "SWE", "date", "2020-07-01 03:30:00.000000+0000", "tso", "RTE", "status", "TEST"),
                Map.of("process", "CORE", "date", "2020-07-01 04:30:00.000000+0000", "tso", "RTE", "status", "TEST")
        );

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        argument.getValue().map(WebSocketMessage::getPayloadAsText).collectList().subscribe(list -> {
            List<Map<String, Object>> expected = refMessages.stream().filter(headers -> {
                String processValue = (String) headers.get("process");
                return process == null || process.equals(processValue);
            }).collect(Collectors.toList());
            List<Map<String, Object>> actual = list.stream().map(t -> {
                try {
                    var deserializedHeaders = ((Map<String, Map<String, Object>>) objectMapper.readValue(t, Map.class))
                            .get("headers");
                    return Map.of("process", deserializedHeaders.get("process"), "date",
                            deserializedHeaders.get("date"));
                } catch (JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
            assertEquals(expected, actual);
        });

        refMessages.stream().map(headers -> new GenericMessage<>("", headers)).forEach(sink::next);
        sink.complete();
    }

    @Test
    public void testWithoutFilter() {
        withFilters(null);
    }

    @Test
    public void testProcessFilter() {
        withFilters("SWE");
    }

    @Test
    public void testHeartbeat() {
        var notificationWebSocketHandler = new MergeNotificationWebSocketHandler(null, 1);

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.handle(ws);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        assertEquals("testsession-0", argument.getValue().blockFirst(Duration.ofSeconds(10)).getPayloadAsText());
    }

}
