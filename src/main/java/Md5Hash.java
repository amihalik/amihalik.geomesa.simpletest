

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Utility methods for generating hashes. Note that MD5 is 16 bytes, or 32 Hex chars. To make it smaller (but still printable), this class
 * Base64 encodes those 16 bytes into 22 chars.
 */
public class Md5Hash {
    public static String md5Base64(byte[] data) {
        return Base64.encodeBase64URLSafeString(DigestUtils.md5(data));
    }

    public static String md5Base64(String string) {
        return md5Base64(StringUtils.getBytesUtf8(string));
    }
}
