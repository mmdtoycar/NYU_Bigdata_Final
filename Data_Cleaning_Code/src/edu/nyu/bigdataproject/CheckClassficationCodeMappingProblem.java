package edu.nyu.bigdataproject;

import edu.nyu.bigdataproject.helper.CodeUtils;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mindaf93 on 4/12/17.
 */
public class CheckClassficationCodeMappingProblem {

    public static List<String> checkAnyCodeMappingProblem(String fileName) {
        Map<String, Map<String, Integer>> codesMap = new HashMap<>();
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
                if (ifValidOffenseCode && ifValidInternalCode) {
                    if (!codesMap.containsKey(internalCode)) {
                        codesMap.put(internalCode, new HashMap<>());
                    }
                    if (codesMap.get(internalCode).containsKey(offenseCode)) {
                        int appearTime = codesMap.get(internalCode).get(offenseCode);
                        codesMap.get(internalCode).put(offenseCode, appearTime + 1);
                    } else{
                        codesMap.get(internalCode).put(offenseCode, 1);
                    }
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        for (String internalKey : codesMap.keySet()) {
            if (codesMap.get(internalKey).size() == 1) {
                continue;
            } else {
                StringBuffer sb = new StringBuffer();
                sb.append(internalKey + ":");
                for (String mappingKey : codesMap.get(internalKey).keySet()) {
                    int tempNum = codesMap.get(internalKey).get(mappingKey);
                    sb.append(String.format("(%s, %d), ", mappingKey, tempNum));
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
                result.add(sb.toString());
            }
        }
        return result;
    }
}
