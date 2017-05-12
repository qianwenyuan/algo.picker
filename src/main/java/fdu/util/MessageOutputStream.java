package fdu.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

/**
 * Created by guoli on 2017/5/10.
 */
public class MessageOutputStream extends OutputStream {

    private UserEndPoint out;
    private OutputStream backup;

    MessageOutputStream(UserEndPoint out) {
        this.out = out;
    }

    MessageOutputStream(OutputStream backup) {
        this.backup = backup;
    }

    public void setOut(UserEndPoint out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        if (out == null) backup.write(b);
        else out.sendMessage(Integer.toString(b));
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (out == null) backup.write(b);
        else out.sendMessage(new String(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (out == null) backup.write(b, off, len);
        else {
            StringWriter s = new StringWriter();
            s.write(new String(b), off, len);
            out.sendMessage(s.toString());
        }
    }
}
