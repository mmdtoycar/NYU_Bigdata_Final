package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mindaf93 on 4/11/17.
 */
public class CheckIfValidThreeDigitCode {

    private static final int LENGTH_OF_CODE = 3;

    public static List<String> checkIfValidThreeDigitCode(String fileName) {
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                String offenseCode = nextLine[6];
                String internalCode = nextLine[8];
                boolean ifValidOffenseCode = judgeIfValidThreeDigitCode(offenseCode);
                boolean ifValidInternalCode = judgeIfValidThreeDigitCode(offenseCode);
                if (!ifValidOffenseCode || !ifValidInternalCode) {
                    result.add(nextLine[0] + "," + offenseCode + "," + internalCode);
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    private static boolean judgeIfValidThreeDigitCode(String code) {
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
