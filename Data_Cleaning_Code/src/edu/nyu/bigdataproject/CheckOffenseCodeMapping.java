package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;
import edu.nyu.bigdataproject.helper.CodeUtils;

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
        Map<String, Map<String, Integer>> codesMap = new HashMap<>();
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String offenseCode = nextLine[6];
                String offenseDetail = nextLine[7];
                boolean ifValidOffenseCode = CodeUtils.judgeIfValidThreeDigitCode(offenseCode);
                if (ifValidOffenseCode) {
                    if (!codesMap.containsKey(offenseCode)) {
                        codesMap.put(offenseCode, new HashMap<>());
                    }
                    if (codesMap.get(offenseCode).containsKey(offenseDetail)) {
                        int appearTime = codesMap.get(offenseCode).get(offenseDetail);
                        codesMap.get(offenseCode).put(offenseDetail, appearTime + 1);
                    } else{
                        codesMap.get(offenseCode).put(offenseDetail, 1);
                    }
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        for (String offenseKey : codesMap.keySet()) {
            if (codesMap.get(offenseKey).size() != 1) {
                StringBuilder sb = new StringBuilder();
                sb.append(offenseKey).append(":");
                for (String mappingKey : codesMap.get(offenseKey).keySet()) {
                    int tempNum = codesMap.get(offenseKey).get(mappingKey);
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
