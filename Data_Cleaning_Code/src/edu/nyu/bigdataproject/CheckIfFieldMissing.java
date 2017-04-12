package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by mindaf93 on 4/11/17.
 */

public class CheckIfFieldMissing {

    private static final int FILED_COUNT = 24;

    public static List<String> checkIfFieldMissing(String fileName) {
        List<String> result = new ArrayList<>();
        // long startTime = System.nanoTime();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');
            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                if (nextLine.length != FILED_COUNT) {
                    result.add(nextLine[0]);
                }
            }
            csvReader.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

//        long endTime = System.nanoTime();
//        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
//        System.out.println("Total elapsed time: " + elapsedTimeInMillis + " ms");
        return result;
    }
}
