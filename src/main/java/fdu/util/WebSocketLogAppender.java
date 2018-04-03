package fdu.util;

import org.apache.log4j.Layout;
import org.apache.log4j.WriterAppender;

import java.io.IOException;
import java.io.OutputStream;

public class WebSocketLogAppender extends WriterAppender {

    private static OutputStream out = new MessageOutputStream(new UserEndPoint<String>() {
        @Override
        public void accept(String s) {
            try {
                UserSessionPool.getInstance().broadcastLog(s);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    });

    public WebSocketLogAppender() {
        setWriter(createWriter(out));
    }

    public WebSocketLogAppender(Layout layout) {
        super(layout, out);
    }

}