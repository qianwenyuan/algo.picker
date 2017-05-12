package fdu.util;

import java.io.IOException;

/**
 * Created by guoli on 2017/5/9.
 */
@FunctionalInterface
public interface UserEndPoint {

    void sendMessage(String s) throws IOException;

}
