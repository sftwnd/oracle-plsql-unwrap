package com.github.sftwnd.oracle.plsql.wrap;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA1 {

    public static byte[] digest(byte[] buff) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA1");
        md.update(buff);
        return md.digest();
    }

}
