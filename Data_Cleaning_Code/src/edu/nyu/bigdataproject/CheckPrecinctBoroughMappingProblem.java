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
public class CheckPrecinctBoroughMappingProblem {
    public static List<String> checkAnyPrecinctBoroughMappingProblem(String fileName) {
        Map<String, Map<String, Integer>> PBMap = new HashMap<>();
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String precinctCode = nextLine[14];
                String boroughName = nextLine[13];
                boolean ifValidPrecinctCode = CodeUtils.judgeIfValidNumber(precinctCode);
                boolean ifValidBoroughName = boroughName != null && boroughName.length() != 0;
                if (ifValidPrecinctCode && ifValidBoroughName) {
                    if (!PBMap.containsKey(precinctCode)) {
                        PBMap.put(precinctCode, new HashMap<>());
                    }
                    if (PBMap.get(precinctCode).containsKey(boroughName)) {
                        int appearTime = PBMap.get(precinctCode).get(boroughName);
                        PBMap.get(precinctCode).put(boroughName, appearTime + 1);
                    } else{
                        PBMap.get(precinctCode).put(boroughName, 1);
                    }
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        for (String precinctKey : PBMap.keySet()) {
            if (PBMap.get(precinctKey).size() != 1) {
                StringBuilder sb = new StringBuilder();
                sb.append(precinctKey).append(":");
                for (String mappingKey : PBMap.get(precinctKey).keySet()) {
                    int tempNum = PBMap.get(precinctKey).get(mappingKey);
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
