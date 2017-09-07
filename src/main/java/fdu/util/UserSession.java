package fdu.util;

import fdu.Config;
import fdu.bean.executor.EmbeddedExecutor;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private BlockingQueue<String> logQueue = new LinkedBlockingQueue<>();

    public UserSession(String sessionID) throws IOException {
        this.sessionID = sessionID;
        scheduleLog();
    }

    public String getSessionID() {
        return sessionID;
    }

    public void initEmbeddedExecutor() throws IOException {
        getEmbeddedExecutor();
    }

    public EmbeddedExecutor getEmbeddedExecutor() {
        if (embeddedExecutor == null) {
            embeddedExecutor = new EmbeddedExecutor(this, getReplOutputStream());
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

    private StringJoiner generateFormData(Map<String, String> arguments) throws UnsupportedEncodingException {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : arguments.entrySet())
            sj.add(URLEncoder.encode(entry.getKey(), "UTF-8") + "="
                    + URLEncoder.encode(entry.getValue(), "UTF-8"));
        return sj;
    }

    public void sendResult(String host, String jid, String data) throws IOException {
        URL url = new URL("http://" + host + Config.RESULT_POST_PATH);

        Map<String, String> arguments = new HashMap<>();
        arguments.put("jid", jid);
        arguments.put("data", data);
        StringJoiner sj = generateFormData(arguments);

        makePost(url, sj.toString(), true);
    }

    private void scheduleLog() throws MalformedURLException {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    URL url = new URL("http://" + Config.getAddress() + Config.LOG_POST_PATH);
                    List<String> buffer = new ArrayList<>();
                    logQueue.drainTo(buffer);
                    if (!buffer.isEmpty()) {
                        Map<String, String> arguments = new HashMap<>();
                        arguments.put("data", String.join("", buffer));
                        StringJoiner sj = generateFormData(arguments);

                        makePost(url, sj.toString(), false);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 500, 2000);
    }

    void sendLog(String s) throws IOException {
        logQueue.offer(s);
    }

    private void makePost(URL url, String content, boolean isForm) throws IOException {
        System.out.println("POSTing to " + url);
        URLConnection con = url.openConnection();
        HttpURLConnection http = (HttpURLConnection) con;
        http.setRequestMethod("POST"); // PUT is another valid option
        http.setDoOutput(true);

        byte[] out = content.getBytes(StandardCharsets.UTF_8);
        int length = out.length;

        http.setFixedLengthStreamingMode(length);
        if (isForm)
            http.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        http.connect();
        try (OutputStream os = http.getOutputStream()) {
            os.write(out);
        }
        http.disconnect();
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
