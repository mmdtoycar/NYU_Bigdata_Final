package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by mindaf93 on 4/11/17.
 */
public class CheckDuplicateCMPNum {

    public static List<String> checkDuplicateCMPNum(String fileName) {
        List<String> result = new ArrayList<>();
        Set<String> set = new HashSet<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');

            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                if (set.contains(nextLine[0])) {
                    result.add(nextLine[0]);
                } else {
                    set.add(nextLine[0]);
                }
            }
            csvReader.close();

        } catch (Exception ex){
            ex.printStackTrace();
        }
        return result;
    }
}
