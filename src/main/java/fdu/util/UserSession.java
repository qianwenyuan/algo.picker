package fdu.util;

import fdu.bean.executor.EmbeddedExecutor;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Created by guoli on 2017/5/9.
 */
public class UserSession {

    private final String sessionID;
    private EmbeddedExecutor embeddedExecutor;
    private MessageOutputStream replOutputStream;

    private WebSocketSession replSession;
    private WebSocketSession resultSession;
    private WebSocketSession logSession;

    private final ResultCache resultCache = new ResultCache();

    public UserSession(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void initEmbeddedExecutor() throws IOException {
        getEmbeddedExecutor();
    }

    public EmbeddedExecutor getEmbeddedExecutor() {
        if (embeddedExecutor == null) {
            /// embeddedExecutor = new EmbeddedExecutor(this, getReplOutputStream(), "spark://10.141.211.91:7077");
            embeddedExecutor = new EmbeddedExecutor(this, getReplOutputStream(), "local[*]");
            embeddedExecutor.init();
        }
        return embeddedExecutor;
    }

    public SparkSession getSparkSession() {
        return getEmbeddedExecutor().spark();
    }

    private MessageOutputStream getReplOutputStream() {
        if (replOutputStream == null)
            replOutputStream = replSession == null ?
                    new MessageOutputStream(System.out) :
                    new MessageOutputStream(replEndPoint);
        return replOutputStream;
    }

    public ResultCache getResultCache() {
        return resultCache;
    }

    public void destroy() throws IOException {
        if (embeddedExecutor != null) embeddedExecutor.destroy();
        replSession.close();
        UserSessionPool.getInstance().removeUserSession(sessionID);
    }

    public void setReplSession(WebSocketSession replSession) throws IOException {
        this.replSession = replSession;
        getReplOutputStream().setOutFunction(replEndPoint);
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
        resultEndPoint.accept(s);
    }

    public void sendLog(String s) throws IOException {
        logEndPoint.accept(s);
    }

    private final UserEndPoint replEndPoint = s -> {
        try {
            if (replSession != null) replSession.sendMessage(new TextMessage(s));
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private final UserEndPoint resultEndPoint = s -> {
        try {
            if (resultSession != null) resultSession.sendMessage(new TextMessage(s));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.print(s);
        }
    };

    private final UserEndPoint logEndPoint = s -> {
        try {
            if (logSession != null) logSession.sendMessage(new TextMessage(s));
        } catch (IOException e) {
            e.printStackTrace();
        }
    };
}
