package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;
import edu.nyu.bigdataproject.helper.CodeUtils;
import edu.nyu.bigdataproject.helper.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mindaf93 on 4/12/17.
 */
public class CheckOffenseCodeMapping {
    public static List<String> checkOffenseCodeMappingProblem(String fileName) {
        Map<String, String> codesMap = new HashMap<>();
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String offenseCode = nextLine[6];
                String offenseDetail = nextLine[7];
                boolean ifValidOffenseCode = CodeUtils.judgeIfValidThreeDigitCode(offenseCode);
                boolean ifValidOffenseDetail = StringUtils.checkIfValidString(offenseDetail);

//                if (!ifValidOffenseCode || !ifValidInternalCode) {
//                    if (codesMap.containsKey(offenseCode)) {
//                        if (codesMap.get(offenseCode).equals(internalCode)) {
//                            continue;
//                        }
//                        result.add(nextLine[0] + "," + offenseCode + "," + internalCode);
//                    } else {
//                        codesMap.put(offenseCode, internalCode);
//                    }
//                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }
}
