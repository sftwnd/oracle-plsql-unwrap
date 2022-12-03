package com.github.sftwnd.oracle.plsql.wrap;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.zip.DataFormatException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UnWrapperTest {

    String loadResource(String name, String extension) throws IOException {
        String resourceFileName = Optional.ofNullable(extension)
                .filter(Predicate.not(String::isBlank))
                .map(ext -> name + "." + ext)
                .orElse(name);
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceFileName)) {
            if (is != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
                byte[] buff = new byte[4096];
                for (int i = is.read(buff); i > 0; i = is.read(buff)) {
                    baos.write(buff, 0, i);
                }
                return baos.toString();
            }
        }
        return null;
    }

    @Test
    void fibTest() throws DataFormatException, IOException, NoSuchAlgorithmException {
        checkResource("fib");
    }

    @Test
    void sampleTest() throws DataFormatException, IOException, NoSuchAlgorithmException {
        checkResource("sample");
    }

    @Test
    void wrapTest() throws DataFormatException, IOException, NoSuchAlgorithmException {
        checkResource("wrap");
    }

    @Test
    void tWin1251Test() throws DataFormatException, IOException, NoSuchAlgorithmException {
        checkResource("t", Charset.forName("windows-1251"));
    }


    void checkResource(String name) throws IOException, DataFormatException, NoSuchAlgorithmException {
        checkResource(name, Charset.defaultCharset());
    }

    void checkResource(String name, Charset charset) throws IOException, DataFormatException, NoSuchAlgorithmException {
        String wrappedText = loadResource(name, "pls");
        assertNotNull(wrappedText, "Unable to load wrapped file: " + name + ".pls");
        String unwrappedText = loadResource(name, "txt");
        assertNotNull(wrappedText, "Unable to load wrapped file: " + name + ".txt");
        String unwrappResult = new String(UnWrapper.unwrap(wrappedText), charset).trim();
        assertEquals(unwrappedText.trim(), unwrappResult.trim(), "Unwrap result is not equals of original code");
    }

}