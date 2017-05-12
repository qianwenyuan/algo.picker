package fdu.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoli on 2017/5/9.
 */
public class UserSessionPool {

    private static UserSessionPool pool;

    private Map<String, UserSession> sessionMap = new HashMap<>();

    public static UserSessionPool getInstance() {
        if (pool == null) {
            pool = new UserSessionPool();
        }
        return pool;
    }

    public UserSession addOrGetUserSession(String sessionId) {
        UserSession session = sessionMap.get(sessionId);
        if (session == null) {
            session = new UserSession(sessionId);
            pool.addUserSession(session);
        }
        return session;
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

//    public void broadcast(String s) throws IOException {
//        for (Map.Entry<String, UserSession> e: sessionMap.entrySet()) {
//            e.getValue().logEndPoint.sendMessage(s);
//        }
//    }

}