package com.github.sftwnd.oracle.plsql.wrap;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

public class UnWrapper {
    private static final int[] charMap = new int[] { 61, 101, 133, 179, 24, 219, 226, 135, 241, 82, 171, 99, 75, 181, 160, 95, 125, 104, 123, 155, 36, 194, 40, 103, 138, 222, 164, 38, 30, 3, 235, 23, 111, 52, 62, 122, 63, 210, 169, 106, 15, 233, 53, 86, 31, 177, 77, 16, 120, 217, 117, 246, 188, 65, 4, 129, 97, 6, 249, 173, 214, 213, 41, 126, 134, 158, 121, 229, 5, 186, 132, 204, 110, 39, 142, 176, 93, 168, 243, 159, 208, 162, 113, 184, 88, 221, 44, 56, 153, 76, 72, 7, 85, 228, 83, 140, 70, 182, 45, 165, 175, 50, 34, 64, 220, 80, 195, 161, 37, 139, 156, 22, 96, 92, 207, 253, 12, 152, 28, 212, 55, 109, 60, 58, 48, 232, 108, 49, 71, 245, 51, 218, 67, 200, 227, 94, 25, 148, 236, 230, 163, 149, 20, 224, 157, 100, 250, 89, 21, 197, 47, 202, 187, 11, 223, 242, 151, 191, 10, 118, 180, 73, 68, 90, 29, 240, 0, 150, 33, 128, 127, 26, 130, 57, 79, 193, 167, 215, 13, 209, 216, 255, 19, 147, 112, 238, 91, 239, 190, 9, 185, 119, 114, 231, 178, 84, 183, 42, 199, 115, 144, 102, 32, 14, 81, 237, 248, 124, 143, 46, 244, 18, 198, 43, 131, 205, 172, 203, 59, 196, 78, 192, 105, 54, 98, 2, 174, 136, 252, 170, 66, 8, 166, 69, 87, 211, 154, 189, 225, 35, 141, 146, 74, 17, 137, 116, 107, 145, 251, 254, 201, 1, 234, 27, 247, 206 };
    private static final Pattern lengthPattern = Pattern.compile("(\n[0-9a-f]+ )([0-9a-f]+\n)");
    public static byte[] unwrap(String wrapped) throws DataFormatException, IOException, NoSuchAlgorithmException {
        String wrappedUnix = wrapped.replace("\r\n", "\n");
        Matcher m = lengthPattern.matcher(wrappedUnix);
        if (!m.find(0)) {
            throw new RuntimeException("Could not unwrap this code. Most probably it was not wrapped with the Oracle 10g, 11g or 12c wrap utility.");
        } else {
            int encodedCodeLength = Integer.parseInt(m.group(2).trim(), 16);
            int expectedLength = m.end() + encodedCodeLength;
            if (expectedLength > wrappedUnix.length()) {
                throw new RuntimeException("Wrapped code seems to be truncated. Expected length of " + expectedLength + " characters but got only " + wrappedUnix.length() + ".");
            } else {
                String encoded = wrappedUnix.substring(m.end(), expectedLength);
                byte[] decoded = BASE64.decodeLines(encoded);
                byte[] remapped = new byte[decoded.length];

                for(int i = 0; i < decoded.length; ++i) {
                    int unsignedInteger = decoded[i] & 0xFF;
                    remapped[i] = (byte) charMap[unsignedInteger];
                }

                byte[] hash = Arrays.copyOfRange(remapped, 0, 20);
                byte[] zipped = Arrays.copyOfRange(remapped, 20, remapped.length);
                byte[] calculatedHash = SHA1.digest(zipped);
                if (!Arrays.equals(hash, calculatedHash)) {
                    throw new RuntimeException("SHA-1 hash values do not match. Expected '" + HEX.toHexString(hash) + "' but got '" + HEX.toHexString(calculatedHash) + "'. Cannot unwrap code.");
                } else {
                    byte[] unzipped = ZIP.unzip(zipped);
                    int size;
                    for(size = unzipped.length; size > 0 && unzipped[size - 1] == 0; --size) {}
                    return Arrays.copyOf(unzipped, size);
                }
            }
        }
    }
}
