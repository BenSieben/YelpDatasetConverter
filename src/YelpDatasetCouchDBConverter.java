import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Scanner;

/**
 * Makes a slightly modified version of the Yelp JSON data to make it the proper format
 * for bulk insertion into CouchDB
 */
public class YelpDatasetCouchDBConverter {

    //Configuration constants (where JSON is located and where to place converted data)
    //File read locations (.json locations)
    public static final String BUSINESS_JSON_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json";

    public static final String CHECKIN_JSON_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_dataset_challenge_round9/yelp_academic_dataset_checkin.json";

    public static final String REVIEW_JSON_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_dataset_challenge_round9/yelp_academic_dataset_review.json";

    public static final String TIP_JSON_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_dataset_challenge_round9/yelp_academic_dataset_tip.json";

    public static final String USER_JSON_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_dataset_challenge_round9/yelp_academic_dataset_user.json";

    //File write locations (conversion locations - THESE SHOULD BE FOLDERS as we must split data into smaller chunks)
    public static final String BUSINESS_CONVERTED_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_couchdb/business/";

    public static final String CHECKIN_CONVERTED_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_couchdb/checkin/";

    public static final String REVIEW_CONVERTED_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_couchdb/review/";

    public static final String TIP_CONVERTED_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_couchdb/tip/";

    public static final String USER_CONVERTED_LOCATION =
            "c:/Users/Ben/Downloads/Yelp Sample Data/yelp_couchdb/user/";

    //Lines per document constant
    public static final int LINES_PER_DOCUMENT = 10000;

    public static void main(String[] args) {
        System.out.println("Yelp Dataset CouchDB Converter");

        // Convert business data
        /*System.out.println("----- Extracting business data from " + BUSINESS_JSON_LOCATION
                + " to " + BUSINESS_CONVERTED_LOCATION + " -----\n");
        convertYelpDataToCouchDBFormat(BUSINESS_JSON_LOCATION, BUSINESS_CONVERTED_LOCATION);


        // Convert checkin data
        System.out.println("----- Extracting checkin data from " + CHECKIN_JSON_LOCATION
                + " to " + CHECKIN_CONVERTED_LOCATION + " -----\n");
        convertYelpDataToCouchDBFormat(CHECKIN_JSON_LOCATION, CHECKIN_CONVERTED_LOCATION);

        // Convert review data
        System.out.println("----- Extracting review data from " + REVIEW_JSON_LOCATION
                + " to " + REVIEW_CONVERTED_LOCATION + " -----\n");
        convertYelpDataToCouchDBFormat(REVIEW_JSON_LOCATION, REVIEW_CONVERTED_LOCATION);

        // Convert tip data
        System.out.println("----- Extracting tip data from " + TIP_JSON_LOCATION
                + " to " + TIP_CONVERTED_LOCATION + " -----\n");
        convertYelpDataToCouchDBFormat(TIP_JSON_LOCATION, TIP_CONVERTED_LOCATION);

        // Convert user data
        System.out.println("----- Extracting user data from " + USER_JSON_LOCATION
                + " to " + USER_CONVERTED_LOCATION + " -----\n");
        convertYelpDataToCouchDBFormat(USER_JSON_LOCATION, USER_CONVERTED_LOCATION);*/

        //Print curl program instructions to bulk insert data
        printCurlInstructions();
    }

    /**
     * Reads a Yelp dataset original JSON file, and converts it to a format for bulk insertion
     * (just make array of Yelp data, and place in object with one key called "docs"
     * @param JSONLocation directory to original data location
     * @param convertLocation directory to place converted data
     */
    private static void convertYelpDataToCouchDBFormat(String JSONLocation, String convertLocation) {
        try {
            //Extract name of data type from converted location
            String[] folders = convertLocation.split("/");
            String dataType = folders[folders.length - 1];
            int fileCounter = 0;  // keeps track of which file number is being written to
            int lineCounter = 0;  // keeps track of liens written to a single file

            //Set up reader / writer objects to retrieve and place text
            BufferedReader reader = new BufferedReader(new FileReader(JSONLocation));
            BufferedWriter writer = new BufferedWriter(new FileWriter(convertLocation + dataType + fileCounter + ".json"));
            Scanner s = new Scanner(reader);



            //Write the data to the .converted file, line-by-line
            while(s.hasNextLine()) {
                String currentLine = s.nextLine();
                if(lineCounter == 0) {
                    //Write first part of converted data (including first document) if first line of file
                    writer.write("{\"docs\":\n[\n" + currentLine);
                }
                else {
                    writer.write(",\n" + currentLine);
                }
                lineCounter++;
                if(lineCounter == LINES_PER_DOCUMENT) {
                    //Write last part of converted data, increase fileCounter, and reset line  counter
                    writer.write("\n]\n}\n");
                    writer.flush();
                    writer.close();
                    lineCounter = 0;
                    fileCounter++;
                    writer = new BufferedWriter(new FileWriter(convertLocation + dataType + fileCounter + ".json"));
                }
            }


            //Write last part of converted data for the last file
            writer.write("\n]\n}\n");

            //Flush writing and close the relevant objects
            writer.flush();
            reader.close();
            writer.close();
            s.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Prints curl program instructions to insert generated CouchDB files into CouchDB
     */
    private static void printCurlInstructions() {
        System.out.println("To insert data into CouchDB, use the following commands (while in directory " +
                "where files were made):\n");
        System.out.println("# Make 'yelp' database:" +
                "\ncurl -X PUT http://127.0.0.1:5984/yelp\n");
        printCurlInstruction(BUSINESS_CONVERTED_LOCATION);
        printCurlInstruction(CHECKIN_CONVERTED_LOCATION);
        printCurlInstruction(REVIEW_CONVERTED_LOCATION);
        printCurlInstruction(TIP_CONVERTED_LOCATION);
        printCurlInstruction(USER_CONVERTED_LOCATION);
    }

    /**
     * Prints out UNIX BASH commands to use to curl the converted Yelp
     * data into CouchDB
     * @param pathToFiles directory
     */
    private static void printCurlInstruction(String pathToFiles) {
        //Extract name of data type from converted location
        String[] folders = pathToFiles.split("/");
        String dataType = folders[folders.length - 1];

        System.out.println("# Insert data in folder " + pathToFiles);
        System.out.println("cd \"" + pathToFiles + "\"");
        System.out.println("for f in " + dataType + "*.json; do");
        System.out.println("curl -X POST -H \"Content-Type: application/json\" " +
                "-d @\"$f\" http://127.0.0.1:5984/yelp/_bulk_docs;");
        System.out.println("done\n\n");
    }
}
