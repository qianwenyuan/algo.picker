package fdu.util;

import fdu.bean.executor.EmbeddedExecutor;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

/**
 * Created by guoli on 2017/5/9.
 */
public class UserSession {

    private final String sessionID;
    private EmbeddedExecutor embeddedExecutor;
    private MessageOutputStream replOutputStream;
    private MessageOutputStream logOutputStream;

    private WebSocketSession replSession;
    private WebSocketSession resultSession;
    private WebSocketSession logSession;

    public UserSession(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void initEmbeddedExecutor() throws IOException {
        getEmbeddedExecutor();
    }

    public EmbeddedExecutor getEmbeddedExecutor() throws IOException {
        if (embeddedExecutor == null) {
            embeddedExecutor = new EmbeddedExecutor(getReplOutputStream());
            embeddedExecutor.init();
        }
        return embeddedExecutor;
    }

    private MessageOutputStream getLogOutputStream() {
        if (logOutputStream == null)
            logOutputStream = logSession == null ?
                    new MessageOutputStream(System.out) :
                    new MessageOutputStream(logEndPoint);
        return logOutputStream;
    }

    private MessageOutputStream getReplOutputStream() throws IOException {
        if (replOutputStream == null)
            replOutputStream = replSession == null ?
                    new MessageOutputStream(System.out) :
                    new MessageOutputStream(replEndPoint);
        return replOutputStream;
    }

    public void destroy() throws IOException {
        if (embeddedExecutor != null) embeddedExecutor.destroy();
        replSession.close();
        UserSessionPool.getInstance().removeUserSession(sessionID);
    }

    public void setReplSession(WebSocketSession replSession) throws IOException {
        this.replSession = replSession;
        getReplOutputStream().setOut(replEndPoint);
    }

    public void setResultSession(WebSocketSession resultSession) {
        this.resultSession = resultSession;
    }

    public void setLogSession(WebSocketSession logSession) {
        this.logSession = logSession;
    }

    public void executeCommand(String s) throws IOException {
        getEmbeddedExecutor().executeCommand(s);
    }

    public void sendResult(String s) throws IOException {
        resultEndPoint.sendMessage(s);
    }

    private final UserEndPoint replEndPoint = new UserEndPoint() {
        @Override
        public void sendMessage(String s) throws IOException {
            replSession.sendMessage(new TextMessage(s));
        }
    };

    private final UserEndPoint resultEndPoint = new UserEndPoint() {
        @Override
        public void sendMessage(String s) throws IOException {
            if (resultSession != null) {
                resultSession.sendMessage(new TextMessage(s));
            } else System.out.print(s);
        }
    };

    private final UserEndPoint logEndPoint = new UserEndPoint() {
        @Override
        public void sendMessage(String s) throws IOException {
            logSession.sendMessage(new TextMessage(s));
        }
    };
}
