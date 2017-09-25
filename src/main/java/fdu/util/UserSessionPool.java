package fdu.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoli on 2017/5/9.
 */
public class UserSessionPool {

    public static final boolean SINGLE_USER = true;
    private static UserSessionPool pool;

    private Map<String, UserSession> sessionMap = new HashMap<>();
    private UserSession singleUser;

    private UserSessionPool() throws IOException {}

    public static UserSessionPool getInstance() throws IOException {
        if (pool == null) {
            pool = new UserSessionPool();
            if (SINGLE_USER) {
                // Individual thread for spark initiation
                // DO NOT Block Spring Boot
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            pool.getSingleUser();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
        }
        return pool;
    }

    private synchronized UserSession getSingleUser() throws IOException {
        if (singleUser == null) {
            singleUser = new UserSession("user");
            this.addUserSession(singleUser);
            singleUser.initEmbeddedExecutor();
        }
        return singleUser;
    }

    public UserSession addOrGetUserSession(String sessionId) throws IOException {
        if (SINGLE_USER) {
            return getSingleUser();
        } else {
            UserSession session = sessionMap.get(sessionId);
            if (session == null) {
                session = new UserSession(sessionId);
                pool.addUserSession(session);
                session.initEmbeddedExecutor();
            }
            return session;
        }
    }

    public void addUserSession(UserSession session) {
        if (sessionMap.get(session.getSessionID()) != null) return;
        sessionMap.put(session.getSessionID(), session);
    }

    public UserSession getUserSession(String uuid) {
        return sessionMap.get(uuid);
    }

    public void removeUserSession(String uuid) {
        sessionMap.remove(uuid);
    }

    public void broadcastLog(String s) throws IOException {
        for (Map.Entry<String, UserSession> e: sessionMap.entrySet()) {
            e.getValue().sendLog(s);
        }
    }

}
