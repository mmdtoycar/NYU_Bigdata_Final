package edu.nyu.bigdataproject;

import edu.nyu.bigdataproject.helper.CodeUtils;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mindaf93 on 4/11/17.
 */
public class CheckIfValidThreeDigitCode {


    public static List<String> checkIfValidThreeDigitCode(String fileName) {
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String offenseCode = nextLine[6];
                String internalCode = nextLine[8];
                boolean ifValidOffenseCode = CodeUtils.judgeIfValidThreeDigitCode(offenseCode);
                boolean ifValidInternalCode = CodeUtils.judgeIfValidThreeDigitCode(internalCode);
//                if (offenseCode.equals("101")) {
//                    result.add(nextLine[0] + "," + offenseCode + "," + internalCode);
//                }
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
}
