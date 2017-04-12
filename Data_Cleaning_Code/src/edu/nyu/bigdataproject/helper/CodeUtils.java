package edu.nyu.bigdataproject.helper;

/**
 * Created by mindaf93 on 4/12/17.
 */
public class CodeUtils {

    private static final int LENGTH_OF_CODE = 3;

    public static boolean judgeIfValidThreeDigitCode(String code) {
        if (code == null || code.length() != LENGTH_OF_CODE) {
            return false;
        }
        for (int i = 0; i < LENGTH_OF_CODE; i++) {
            char thisChar = code.charAt(i);
            if (thisChar < '0' || thisChar > '9') {
                return false;
            }
        }
        return true;
    }
}
