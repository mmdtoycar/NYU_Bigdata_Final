package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;
import edu.nyu.bigdataproject.helper.DateUtils;
import edu.nyu.bigdataproject.helper.ResultType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mindaf93 on 4/11/17.
 */
public class CheckReportTimeValid {
    public static List<String> checkReportTimeValid(String fileName) {
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');

            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                if (!ifThisReportDateIsValid(nextLine[5])) {
                    result.add(nextLine[0] + "," + nextLine[5]);
                }
            }
            csvReader.close();

        } catch (Exception ex){
            ex.printStackTrace();
        }
        return result;
    }

    private static boolean ifThisReportDateIsValid(String reportDate) {
        boolean ifHasReportDateAndTime = DateUtils.ifThisDateExists(reportDate, "00:00:00");
        ResultType reportDateAndTimeResult = null;
        if (ifHasReportDateAndTime) {
            reportDateAndTimeResult = DateUtils.ifThisDateIsValid(reportDate, "00:00:00");
            if (!reportDateAndTimeResult.valid) {
                return false;
            }
        }
        return true;
    }
}
