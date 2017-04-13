package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created by mindaf93 on 4/12/17.
 */
public class CheckJurisdictionDistribution {

    public static List<String> checkAllJurisdictionDistribution(String fileName) {
        List<String> result = new ArrayList<>();
        Map<String, Integer> jurisdictionMap = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String jurisdictionResult = nextLine[12];
                if (!jurisdictionMap.containsKey(jurisdictionResult)) {
                    jurisdictionMap.put(jurisdictionResult, 0);
                }
                int count = jurisdictionMap.get(jurisdictionResult);
                jurisdictionMap.put(jurisdictionResult, count + 1);
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        for (Map.Entry<String, Integer> entry : jurisdictionMap.entrySet()) {
            result.add(String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
        return result;
    }

}
