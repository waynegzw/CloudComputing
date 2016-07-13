import java.math.BigInteger;

/**
 * Created by Zhangwei on 2/27/16.
 */
public class Decipher {
    private static final BigInteger SECRETKEY
            = new BigInteger("64266330917908644872330635228106713310880186591609208114244758680898150367880703152525200743234420230");

    private int getOffset(String messageKey) {
        BigInteger key = new BigInteger(messageKey);
        BigInteger gcd = SECRETKEY.gcd(key);
        int offset = 1 + gcd.intValue() % 25;
        return offset;
    }

    private char cesarShift(char c, int offset) {
        return (char) (c - offset < 65 ? c - offset + 26 : c - offset);
    }

    public String decipher(String encryptedStr, String messageKey) {

        int offset = getOffset(messageKey);

        char[] encryptedArr = encryptedStr.toCharArray();
        int len = encryptedArr.length;
        int matrixLen = (int) Math.sqrt(len);
        int top = 0, bottom = matrixLen - 1, left = 0, right = matrixLen - 1;
        int direction = 0;
        char[] result = new char[len];
        int k = 0;
        char c;
        while (top <= bottom && left <= right) {
            if (direction == 0) {
                for (int i = left; i <= right; i++) {
                    result[k++] = cesarShift(encryptedArr[top * matrixLen + i], offset);
                }
                top++;
                direction = 1;
            } else if (direction == 1) {
                for (int i = top; i <= bottom; i++) {
                    result[k++] = cesarShift(encryptedArr[i * matrixLen + right], offset);
                }
                right--;
                direction = 2;
            } else if (direction == 2) {
                for (int i = right; i >= left; i--) {
                    result[k++] = cesarShift(encryptedArr[bottom * matrixLen + i], offset);
                }
                bottom--;
                direction = 3;
            } else {
                for (int i = bottom; i >= top; i--) {
                    result[k++] = cesarShift(encryptedArr[i * matrixLen + left], offset);
                }
                left++;
                direction = 0;
            }
        }
        return new String(result);
    }

    public static void main(String[] args) {
        System.out.println(new Decipher().decipher("URYEXYBJB", "4024123659485622445001958636275419709073611535463684596712464059093821"));
    }
}
