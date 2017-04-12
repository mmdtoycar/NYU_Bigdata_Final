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
public class CheckPDCodeMapping {
    
    public static List<String> checkPDCodeMappingProblem(String fileName) {
        Map<String, Map<String, Integer>> codesMap = new HashMap<>();
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String PDCode = nextLine[8];
                String PDDetail = nextLine[9];
                boolean ifValidPDCode = CodeUtils.judgeIfValidThreeDigitCode(PDCode);
                if (ifValidPDCode) {
                    if (!codesMap.containsKey(PDCode)) {
                        codesMap.put(PDCode, new HashMap<>());
                    }
                    if (codesMap.get(PDCode).containsKey(PDDetail)) {
                        int appearTime = codesMap.get(PDCode).get(PDDetail);
                        codesMap.get(PDCode).put(PDDetail, appearTime + 1);
                    } else{
                        codesMap.get(PDCode).put(PDDetail, 1);
                    }
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        for (String PDKey : codesMap.keySet()) {
            if (codesMap.get(PDKey).size() != 1) {
                StringBuilder sb = new StringBuilder();
                sb.append(PDKey).append(":");
                for (String mappingKey : codesMap.get(PDKey).keySet()) {
                    int tempNum = codesMap.get(PDKey).get(mappingKey);
                    sb.append(String.format("(%s, %d), ", mappingKey, tempNum));
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
                result.add(sb.toString());
            } else {
                String detail =  null;
                for (String detailString : codesMap.get(PDKey).keySet()) {
                    detail = detailString;
                }
                if (detail != null && detail.equals("")) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(PDKey).append(":");
                    int tempNum = codesMap.get(PDKey).get(detail);
                    sb.append(String.format("(%s, %d)", detail, tempNum));
                    result.add(sb.toString());
                }
            }
        }
        return result;
    }
}
