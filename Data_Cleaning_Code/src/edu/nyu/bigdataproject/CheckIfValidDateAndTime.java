package edu.nyu.bigdataproject;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

            String [] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                if (!ifThisRowIsValid(nextLine[1], nextLine[2], nextLine[3], nextLine[4])) {
                    result.add(nextLine[0] + "," + nextLine[1] + "," + nextLine[2] + "," + nextLine[3] + "," + nextLine[4]);
                }
            }
            csvReader.close();

        } catch (Exception ex){
            ex.printStackTrace();
        }
        return result;
    }

    private static boolean ifThisRowIsValid(String startDate, String startTime, String endDate, String endTime) {
        boolean ifHasStartDateAndTime = ifThisDateExists(startDate, startTime);
        boolean ifHasEndDateAndTime = ifThisDateExists(endDate, endTime);
        ResultType startDateAndTimeResult = null;
        ResultType endDateAndTimeResult = null;
        if (ifHasStartDateAndTime) {
            startDateAndTimeResult = ifThisDateIsValid(startDate, startTime);
            if (!startDateAndTimeResult.valid) {
                return false;
            }
        }
        if (ifHasEndDateAndTime) {
            endDateAndTimeResult = ifThisDateIsValid(endDate, endTime);
            if (!endDateAndTimeResult.valid) {
                return false;
            }
        }
        if (ifHasStartDateAndTime && ifHasEndDateAndTime) {
            return (startDateAndTimeResult.date <= endDateAndTimeResult.date);
        }
        return false;
    }

    private static ResultType ifThisDateIsValid(String date, String time) {
        long dateTime = 0;
        if (date == null || time == null) {
            return new ResultType(false, dateTime);
        }
        String[] dateArray = date.split("/");
        String[] timeArray = time.split(":");
        if (dateArray.length != 3 || timeArray.length != 3) {
            return new ResultType(false, dateTime);
        }
        String year, month, day, hour, minute, second;
        try {
            int intYear = Integer.valueOf(dateArray[2]);
            if (intYear < 0 || intYear > 99) {
                throw new IndexOutOfBoundsException();
            }
            year = "20" + String.valueOf(intYear);

            int intMonth = Integer.valueOf(dateArray[0]);
            if (intMonth < 1 || intMonth > 12) {
                throw new IndexOutOfBoundsException();
            }
            if (intMonth < 10) {
                month = "0" + String.valueOf(intMonth);
            } else {
                month = String.valueOf(intMonth);
            }

            int intDay = Integer.valueOf(dateArray[1]);
            if (intDay < 1 || intDay > 31) {
                throw new IndexOutOfBoundsException();
            }
            if (intDay < 10) {
                day = "0" + String.valueOf(intDay);
            } else {
                day = String.valueOf(intDay);
            }

            int intHour = Integer.valueOf(timeArray[0]);
            if (intHour < 0 || intHour > 23) {
                throw new IndexOutOfBoundsException();
            }
            if (intHour < 10) {
                hour = "0" + String.valueOf(intHour);
            } else {
                hour = String.valueOf(intHour);
            }

            int intMinute = Integer.valueOf(timeArray[1]);
            if (intMinute < 0 || intMinute > 59) {
                throw new IndexOutOfBoundsException();
            }
            if (intHour < 10) {
                minute = "0" + String.valueOf(intMinute);
            } else {
                minute = String.valueOf(intMinute);
            }

            int intSecond = Integer.valueOf(timeArray[2]);
            if (intSecond < 0 || intSecond > 59) {
                throw new IndexOutOfBoundsException();
            }
            if (intSecond < 10) {
                second = "0" + String.valueOf(intSecond);
            } else {
                second = String.valueOf(intSecond);
            }

            String thisDateString = String.format("%s/%s/%s-%s:%s:%s", month, day, year, hour, minute, second);
            Date thisDate = convertThisTimeToDateType(thisDateString);
            dateTime = thisDate.getTime();
        } catch (Exception ex) {
            return new ResultType(true, dateTime);
        }
        return new ResultType(true, dateTime);
    }

    private static boolean ifThisDateExists(String date, String time) {
        return !date.equals("") || !time.equals("");
    }

    private static Date convertThisTimeToDateType(String timeString) throws ParseException{
        DateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy-hh:mm:ss");
        return dateFormatter.parse(timeString);
    }
}

class ResultType {
    public final boolean valid;
    public final long date;
    ResultType(boolean valid, long date) {
        this.valid = valid;
        this.date = date;
    }
}
