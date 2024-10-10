/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.merge.notification.server;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.StandardWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = {MergeNotificationApplication.class})
@DirtiesContext
class MergeNotificationWebSocketIT {

    @LocalServerPort
    private String port;

    @Test
    void echo() {
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        client.execute(getUrl("/notify"), WebSocketSession::close).block();
    }

    private URI getUrl(String path) {
        return URI.create("ws://localhost:" + this.port + path);
    }
}
