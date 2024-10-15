/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.merge.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
class MergeNotificationWebSocketHandlerTest {

    private ObjectMapper objectMapper;
    private WebSocketSession ws;
    private HandshakeInfo handshakeinfo;
    private UriComponentsBuilder uriComponentBuilder;

    private static final String SWE_UUID = "11111111-f60e-4766-bc5c-8f312c1984e4";
    private static final String CORE_UUID = "21111111-f60e-4766-bc5c-8f312c1984e4";

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
        var dataBufferFactory = new DefaultDataBufferFactory();

        ws = Mockito.mock(WebSocketSession.class);
        handshakeinfo = Mockito.mock(HandshakeInfo.class);
        uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

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

    private void withFilters(String processUuid, String businessProcess) {
        var notificationWebSocketHandler = new MergeNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);

        if (processUuid != null) {
            uriComponentBuilder.queryParam("processUuid", processUuid);
        }
        if (businessProcess != null) {
            uriComponentBuilder.queryParam("businessProcess", businessProcess);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());

        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();

        notificationWebSocketHandler.handle(ws);

        List<Map<String, Object>> refMessages = Arrays.asList(
                Map.of("processUuid", SWE_UUID, "businessProcess", "1D", "date", "2020-07-01 02:30:00.000000+0000", "tso", "RTE", "status", "TEST"),
                Map.of("processUuid", SWE_UUID, "businessProcess", "2D", "date", "2020-07-01 03:30:00.000000+0000", "tso", "RTE", "status", "TEST"),
                Map.of("processUuid", CORE_UUID, "businessProcess", "SN", "date", "2020-07-01 04:30:00.000000+0000", "tso", "RTE", "status", "TEST")
        );

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        List<String> messages = new ArrayList<>();
        argument.getValue().map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);
        refMessages.stream().map(headers -> new GenericMessage<>("", headers)).forEach(sink::next);
        sink.complete();

        List<Map<String, Object>> expected = refMessages.stream().filter(headers -> {
            String processValue = (String) headers.get("processUuid");
            String businessProcessValue = (String) headers.get("businessProcess");
            return (processUuid == null || processUuid.equals(processValue)) && (businessProcess == null || businessProcess.equals(businessProcessValue));
        }).collect(Collectors.toList());

        List<Map<String, Object>> actual = messages.stream().map(t -> {
            try {
                var deserializedHeaders = ((Map<String, Map<String, Object>>) objectMapper.readValue(t, Map.class)).get("headers");
                return Map.of("processUuid", deserializedHeaders.get("processUuid"),
                        "businessProcess", deserializedHeaders.get("businessProcess"),
                        "date", deserializedHeaders.get("date"),
                        "status", deserializedHeaders.get("status"),
                        "tso", deserializedHeaders.get("tso"));

            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void testWithoutFilter() {
        withFilters(null, null);
    }

    @Test
    void testProcessFilter() {
        withFilters(SWE_UUID, "1D");
    }

    @Test
    void testHeartbeat() {
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

    @Test
    void testDiscard() {
        var notificationWebSocketHandler = new MergeNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);

        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());

        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        Map<String, Object> headers = Map.of("processUuid", SWE_UUID, "businessProcess", "1D", "date", "2020-07-01 02:30:00.000000+0000", "tso", "RTE", "status", "TEST");

        sink.next(new GenericMessage<>("", headers)); // should be discarded, no client connected

        notificationWebSocketHandler.handle(ws);

        ArgumentCaptor<Flux<WebSocketMessage>> argument1 = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument1.capture());
        List<String> messages1 = new ArrayList<String>();
        Flux<WebSocketMessage> out1 = argument1.getValue();
        Disposable d1 = out1.map(WebSocketMessage::getPayloadAsText).subscribe(messages1::add);
        d1.dispose();

        sink.next(new GenericMessage<>("", headers)); // should be discarded, first client disconnected

        notificationWebSocketHandler.handle(ws);

        ArgumentCaptor<Flux<WebSocketMessage>> argument2 = ArgumentCaptor.forClass(Flux.class);
        verify(ws, times(2)).send(argument2.capture());
        List<String> messages2 = new ArrayList<String>();
        Flux<WebSocketMessage> out2 = argument2.getValue();
        Disposable d2 = out2.map(WebSocketMessage::getPayloadAsText).subscribe(messages1::add);
        d2.dispose();

        sink.complete();
        assertEquals(0, messages1.size());
        assertEquals(0, messages2.size());
    }
}
