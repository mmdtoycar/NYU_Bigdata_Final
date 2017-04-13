package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mindaf93 on 4/13/17.
 */
public class CheckDescriptionOfPremiseDistribution {
    public static List<String> checkAllDescriptionOfPremiseDistribution(String fileName) {
        List<String> result = new ArrayList<>();
        Map<String, Integer> distributionMap = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String premiseResult = nextLine[16].trim();
                if (!distributionMap.containsKey(premiseResult)) {
                    distributionMap.put(premiseResult, 0);
                }
                int count = distributionMap.get(premiseResult);
                distributionMap.put(premiseResult, count + 1);
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        for (Map.Entry<String, Integer> entry : distributionMap.entrySet()) {
            result.add(String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
        return result;
    }
}
