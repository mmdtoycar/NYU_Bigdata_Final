package edu.nyu.bigdataproject;

import java.util.List;

public class Main {

    private static final String FILE_PATH = "./NYPD_Complaint_Data_Historic.csv";

    public static void main(String[] args) {
        // write your code here
//
//        checkIfExistsWrongNumberOfFields();
//
//        checkIfExistsDuplicateCMPNum();
//
//        checkIfExistsInvalidDate();
//
//        checkIfReportTimeValid();
//
//        checkIfValidClassificationCode();
//
        checkIfExistsWrongCodesMapping();

    }

    private static void checkIfExistsWrongCodesMapping() {
        System.out.println("\n[Report] Detecting if wrong code mapping records...");
        List<String> ifExistsWrongCodesMappingProblem =
                CheckClassficationCodeMappingProblem.checkAnyCodeMappingProblem(FILE_PATH);
        if (ifExistsWrongCodesMappingProblem.size() == 0) {
            System.out.println("[Pass] No wrong code mapping error is found!");
        } else {
            System.out.println(
                    String.format("[Alert] The following %s row(s) of data contains " +
                                    "wrong code mapping record:",
                            ifExistsWrongCodesMappingProblem.size()));
            for (String eachLine : ifExistsWrongCodesMappingProblem) {
                System.out.println(eachLine);
            }
            System.out.println(
                    String.format("[Alert] The following %s row(s) of data contains " +
                                    "wrong code mapping record:",
                            ifExistsWrongCodesMappingProblem.size()));
        }
    }

    private static void checkIfValidClassificationCode() {
        System.out.println("\n[Report] Detecting if wrong three digit classification code records...");
        List<String> ifExistsInvalidClassificationDate =
                CheckIfValidThreeDigitCode.checkIfValidThreeDigitCode(FILE_PATH);
        if (ifExistsInvalidClassificationDate.size() == 0) {
            System.out.println("[Pass] No wrong three digit classification code error is found!");
        } else {
            System.out.println(
                    String.format("[Alert] The following %s row(s) of data contains " +
                                    "wrong three digit classification code record:",
                            ifExistsInvalidClassificationDate.size()));
            for (String eachLine : ifExistsInvalidClassificationDate) {
                System.out.println(eachLine);
            }
        }
    }

    private static void checkIfReportTimeValid() {
        System.out.println("\n[Report] Detecting if wrong report date records...");
        List<String> ifExistsInvalidReportDate = CheckReportTimeValid.checkReportTimeValid(FILE_PATH);
        if (ifExistsInvalidReportDate.size() == 0) {
            System.out.println("[Pass] No wrong report date error is found!");
        } else {
            System.out.println(
                    String.format("[Alert] The following %s row(s) of data contains wrong report date record:",
                            ifExistsInvalidReportDate.size()));
            for (String eachLine : ifExistsInvalidReportDate) {
                System.out.println(eachLine);
            }
        }
    }

    private static void checkIfExistsInvalidDate() {
        System.out.println("\n[Report] Detecting if exists wrong date records...");
        List<String> ifExistsInvalidDate = CheckIfValidDateAndTime.checkIfValidDateAndTime(FILE_PATH);
        if (ifExistsInvalidDate.size() == 0) {
            System.out.println("[Pass] No wrong date error is found!");
        } else {
            System.out.println(String.format("[Alert] The following %s row(s) of data contains wrong date record:",
                    ifExistsInvalidDate.size()));
            for (String eachLine : ifExistsInvalidDate) {
                System.out.println(eachLine);
            }
        }
    }

    private static void checkIfExistsDuplicateCMPNum() {
        System.out.println("\n[Report] Detecting if exists any duplicate CMP number...");
        List<String> ifExistsDuplicateCMPNum = CheckDuplicateCMPNum.checkDuplicateCMPNum(FILE_PATH);
        if (ifExistsDuplicateCMPNum.size() == 0) {
            System.out.println("[Pass] No duplicate complaint number found!");
        } else {
            System.out.println("[Alert] The following row(s) of data contains duplicate complaint number:");
            for (String eachLine : ifExistsDuplicateCMPNum) {
                System.out.println(eachLine);
            }
        }
    }

    private static void checkIfExistsWrongNumberOfFields() {
        System.out.println("\n[Report] Detecting if all rows of data contains correct number of fields...");
        List<String> ifAnyRowDoesNotContainTheExactNumberOfFields = CheckIfFieldMissing.checkIfFieldMissing(FILE_PATH);
        if (ifAnyRowDoesNotContainTheExactNumberOfFields.size() == 0) {
            System.out.println("[Pass] All rows of data contains correct number of fields!");
        } else {
            System.out.println("[Alert] The following row(s) of data contains incorrect number of fields");
            for (String eachLine : ifAnyRowDoesNotContainTheExactNumberOfFields) {
                System.out.println(eachLine);
            }
        }
    }
}

