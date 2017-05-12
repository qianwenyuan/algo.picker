package fdu.service;

import fdu.util.UserSession;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Created by guoli on 2017/5/10.
 */
public class ResultWebSocketHandler extends TextWebSocketHandler {

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        UserSession userSession = (UserSession) session.getAttributes().get("session");
        userSession.setResultSession(session);
    }

}
