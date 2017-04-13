package edu.nyu.bigdataproject;

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
public class CheckBoroughDistribution {
    public static List<String> checkAllBoroughDistribution(String fileName) {
        List<String> result = new ArrayList<>();
        Map<String, Integer> boroughMap = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String boroughResult = nextLine[13];
                if (!boroughMap.containsKey(boroughResult)) {
                    boroughMap.put(boroughResult, 0);
                }
                int count = boroughMap.get(boroughResult);
                boroughMap.put(boroughResult, count + 1);
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        for (Map.Entry<String, Integer> entry : boroughMap.entrySet()) {
            result.add(String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
        return result;
    }
}
