package com.github.sftwnd.oracle.plsql.wrap;

public class HEX {

    public static String toHexString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];

        for(int i = 0; i < bytes.length; ++i) {
            int v = bytes[i] & 0xFF;
            hexChars[i << 1] = hexChars[v >>> 4];
            hexChars[(i << 1) + 1] = hexChars[v & 0x0F];
        }

        return new String(hexChars);
    }

}
