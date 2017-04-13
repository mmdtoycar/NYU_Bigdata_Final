package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mindaf93 on 4/12/17.
 */
public class CheckLocationOfOccurrenceRange {
    public static List<String> checkLocationOfOccurrenceRange(String fileName, List<String> rangeList) {
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                String locationOfOccurrence = nextLine[15].trim();
                if (!rangeList.contains(locationOfOccurrence)) {
                    result.add(String.format("%s: %s", nextLine[0], nextLine[15]));
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return result;
    }
}
