package edu.nyu.bigdataproject;

import java.util.List;

public class Main {

    private static final String FILE_PATH = "./NYPD_Complaint_Data_Historic.csv";

    public static void main(String[] args) {
        // write your code here
        List<String> ifAnyRowDoesNotContainTheExactNumberOfFields = CheckIfFieldMissing.checkIfFieldMissing(FILE_PATH);

        if (ifAnyRowDoesNotContainTheExactNumberOfFields.size() == 0) {
            System.out.println("All rows of data contains correct number of fields");
        } else {
            System.out.println("The following row(s) of data contains incorrect number of fields");
            for (String eachLine : ifAnyRowDoesNotContainTheExactNumberOfFields) {
                System.out.println(eachLine);
            }
            System.out.println("");
        }

//        long startTime = System.nanoTime();
//        Set<String> set = new HashSet<>();
//        try {
//            Stream<String> lines = Files.lines( file, StandardCharsets.UTF_8 );
//
//            for( String line : (Iterable<String>) lines::iterator ) {
//                String[] temp = line.split(",");
//                if (set.contains(temp[0])) {
//                    System.out.println(line);
//                } else {
//                    set.add(temp[0]);
//                }
//                if (temp.length != 24) {
//                    System.out.println(line);
//                }
//            }
//
//
//        } catch (IOException ioe){
//            ioe.printStackTrace();
//        }
    }
}

