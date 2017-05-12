package fdu.service;

import fdu.util.UserSession;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Created by guoli on 2017/5/10.
 */
public class ReplWebSocketHandler extends TextWebSocketHandler {

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        UserSession userSession = (UserSession) session.getAttributes().get("session");
        userSession.executeCommand(message.getPayload());
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        UserSession userSession = (UserSession) session.getAttributes().get("session");
        userSession.setReplSession(session);
        userSession.initEmbeddedExecutor();
    }
}
