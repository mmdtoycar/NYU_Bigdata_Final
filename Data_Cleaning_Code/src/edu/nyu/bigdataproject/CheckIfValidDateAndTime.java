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
public class CheckIfValidDateAndTime {

    public static List<String> checkIfValidDateAndTime(String fileName) {
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            CSVReader csvReader = new CSVReader(reader, ',');

            String[] nextLine = csvReader.readNext(); // read the header line
            while ((nextLine = csvReader.readNext()) != null) {
                if (!ifThisRowIsValid(nextLine[1], nextLine[2], nextLine[3], nextLine[4])) {
                    result.add(nextLine[0] + "," + nextLine[1]
                            + "," + nextLine[2] + "," + nextLine[3] + "," + nextLine[4]);
                }
            }
            csvReader.close();

        } catch (Exception ex){
            ex.printStackTrace();
        }
        return result;
    }

    private static boolean ifThisRowIsValid(String startDate, String startTime, String endDate, String endTime) {
        boolean ifHasStartDateAndTime = DateUtils.ifThisDateExists(startDate, startTime);
        boolean ifHasEndDateAndTime = DateUtils.ifThisDateExists(endDate, endTime);
        ResultType startDateAndTimeResult = null;
        ResultType endDateAndTimeResult = null;
        if (ifHasStartDateAndTime) {
            startDateAndTimeResult = DateUtils.ifThisDateIsValid(startDate, startTime);
            if (!startDateAndTimeResult.valid) {
                return false;
            }
        }
        if (ifHasEndDateAndTime) {
            endDateAndTimeResult = DateUtils.ifThisDateIsValid(endDate, endTime);
            if (!endDateAndTimeResult.valid) {
                return false;
            }
        }
        if (ifHasStartDateAndTime && ifHasEndDateAndTime) {
            return (startDateAndTimeResult.date <= endDateAndTimeResult.date);
        } else if (!ifHasStartDateAndTime && !ifHasEndDateAndTime) {
            return false;
        }
        return true;
    }
}


