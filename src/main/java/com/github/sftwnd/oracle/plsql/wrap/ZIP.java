package com.github.sftwnd.oracle.plsql.wrap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class ZIP {

    public static byte[] unzip(byte[] zipped) throws DataFormatException, IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(zipped);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(zipped.length);
        byte[] buffer = new byte[1024];
        while(!inflater.finished()) {
            int count = inflater.inflate(buffer);
            baos.write(buffer, 0, count);
        }
        baos.flush();
        baos.close();
        return baos.toByteArray();
    }

}
