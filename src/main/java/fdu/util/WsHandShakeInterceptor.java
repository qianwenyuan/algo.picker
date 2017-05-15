package fdu.util;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

import javax.servlet.http.HttpSession;
import java.util.Map;

/**
 * Created by guoli on 2017/5/10.
 */

public class WsHandShakeInterceptor extends HttpSessionHandshakeInterceptor {

    @Override
    public boolean beforeHandshake(ServerHttpRequest request,
                                   ServerHttpResponse response,
                                   WebSocketHandler wsHandler,
                                   Map<String, Object> attributes) throws Exception {
        HttpSession httpSession = ((ServletServerHttpRequest) request)
                .getServletRequest()
                .getSession();
        UserSession session = UserSessionPool.getInstance()
                .addOrGetUserSession(httpSession.getId());
        attributes.put("session", session);
        return super.beforeHandshake(request, response, wsHandler, attributes);
    }

}
