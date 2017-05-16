import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Scanner;

/**
 * Converter for the .json data from
 * the dataset provided by Yelp,
 * as MySQL needs a specific format
 * to read in the data correctly
 */
public class YelpDatasetConverter {

    //Configuration constants (where JSON is located and where to place converted data)
    //File read locations (.json locations)
    public static final String BUSINESS_JSON_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_dataset_challenge_round9\\yelp_academic_dataset_business.json";

    public static final String CHECKIN_JSON_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_dataset_challenge_round9\\yelp_academic_dataset_checkin.json";

    public static final String REVIEW_JSON_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_dataset_challenge_round9\\yelp_academic_dataset_review.json";

    public static final String TIP_JSON_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_dataset_challenge_round9\\yelp_academic_dataset_tip.json";

    public static final String USER_JSON_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_dataset_challenge_round9\\yelp_academic_dataset_user.json";

    //File write locations (conversion locations)
    public static final String BUSINESS_CONVERTED_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_me\\business.txt";

    public static final String CHECKIN_CONVERTED_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_me\\checkin.txt";

    public static final String REVIEW_CONVERTED_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_me\\review.txt";

    public static final String TIP_CONVERTED_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_me\\tip.txt";

    public static final String USER_CONVERTED_LOCATION =
            "c:\\Users\\Ben\\Downloads\\Yelp Sample Data\\yelp_me\\user.txt";

    // separator used to separate each column (traditionally a comma for .csv files)
    public static final String SEPARATOR = "|||";
    //public static final String SEPARATOR = ",";
    //public static final String SEPARATOR = "\t";  // horizontal tab separator

    // escape used for quotations and commas (for certain data translation processes)
    //public static final String ESCAPE_CHAR = "\\\\";
    public static final String ESCAPE_CHAR = "|";

    //String arrays for the different Yelp datasets (to indicate name / order of data to extract from JSON files)
    public static final String[] BUSINESS_COLUMN_NAMES = {"business_id", "name", "neighborhood", "address", "city", "state",
            "postal_code", "latitude", "longitude", "stars", "review_count", "is_open", "attributes",
            "categories", "hours", "type"};

    public static final String[] CHECKIN_COLUMN_NAMES = {"time", "business_id", "type"};

    public static final String[] REVIEW_COLUMN_NAMES = {"review_id", "user_id", "business_id", "stars", "date", "text",
            "useful", "funny", "cool", "type"};

    public static final String[] TIP_COLUMN_NAMES = {"text", "date", "likes", "business_id", "user_id", "type"};

    public static final String[] USER_COLUMN_NAMES = {"user_id", "name", "review_count", "yelping_since", "friends",
            "useful", "funny", "cool", "fans", "elite", "average_stars", "compliment_hot", "compliment_more",
            "compliment_profile", "compliment_cute", "compliment_list", "compliment_note", "compliment_plain",
            "compliment_cool", "compliment_funny", "compliment_writer", "compliment_photos", "type"};

    public static void main(String[] args) {
        System.out.println("Yelp Dataset Converter\n");

        // Convert business data
        System.out.println("----- Extracting business data from " + BUSINESS_JSON_LOCATION
            + " to " + BUSINESS_CONVERTED_LOCATION + " -----\n");
        //readLinesOfFile(BUSINESS_JSON_LOCATION, 20);
        convertJSON(BUSINESS_JSON_LOCATION, BUSINESS_CONVERTED_LOCATION, BUSINESS_COLUMN_NAMES, SEPARATOR);
        //readLinesOfFile(BUSINESS_CONVERTED_LOCATION, 20);

        // Convert checkin data
        System.out.println("----- Extracting checkin data from " + CHECKIN_JSON_LOCATION
                + " to " + CHECKIN_CONVERTED_LOCATION + " -----\n");
        //readLinesOfFile(CHECKIN_JSON_LOCATION, 20);
        convertJSON(CHECKIN_JSON_LOCATION, CHECKIN_CONVERTED_LOCATION, CHECKIN_COLUMN_NAMES, SEPARATOR);
        //readLinesOfFile(CHECKIN_CONVERTED_LOCATION, 20);

        // Convert review data
        System.out.println("----- Extracting review data from " + REVIEW_JSON_LOCATION
                + " to " + REVIEW_CONVERTED_LOCATION + " -----\n");
        //readLinesOfFile(REVIEW_JSON_LOCATION, 20);
        convertJSON(REVIEW_JSON_LOCATION, REVIEW_CONVERTED_LOCATION, REVIEW_COLUMN_NAMES, SEPARATOR);
        //readLinesOfFile(REVIEW_CONVERTED_LOCATION, 20);

        // Convert tip data
        System.out.println("----- Extracting tip data from " + TIP_JSON_LOCATION
                + " to " + TIP_CONVERTED_LOCATION + " -----\n");
        //readLinesOfFile(TIP_JSON_LOCATION, 20);
        convertJSON(TIP_JSON_LOCATION, TIP_CONVERTED_LOCATION, TIP_COLUMN_NAMES, SEPARATOR);
        //readLinesOfFile(TIP_CONVERTED_LOCATION, 20);

        // Convert user data
        System.out.println("----- Extracting user data from " + USER_JSON_LOCATION
                + " to " + USER_CONVERTED_LOCATION + " -----\n");
        //readLinesOfFile(USER_JSON_LOCATION, 20);
        convertJSON(USER_JSON_LOCATION, USER_CONVERTED_LOCATION, USER_COLUMN_NAMES, SEPARATOR);
        //readLinesOfFile(USER_CONVERTED_LOCATION, 20);
    }

    private static void readLinesOfFile(String location, int numLines) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(location));
            Scanner in = new Scanner(reader);
            int lineCount = 0;
            while(in.hasNextLine() && lineCount++ < numLines) {
                String line = in.nextLine();
                System.out.println(line);
                String[] columns = line.split("[\\t]");  // Split by tab character (for MySQL Dump CSVs)
                System.out.println("Columns\n" + stringArrayToString(columns) + "\n");
            }
            System.out.println();
            in.close();
            reader.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    private static String stringArrayToString(String[] arr) {
        String output = arr[0];
        for (int i = 1; i < arr.length; i++) {
            output += ", " + arr[i];
        }
        return output;
    }

    /**
     * Converts a Yelp JSON to a more MySQL-friendly format
     * @param JSONLocation where the Yelp data JSON file is located
     * @param convertLocation where to save the converted data
     * @param columnNames list of all the different columns in the chosen Yelp data (make sure order is matching Yelp JSON order!)
     * @param separator how to separate the values (ex: CSV would normally use a comma to separate each data)
     */
    private static void convertJSON(String JSONLocation, String convertLocation, String[] columnNames, String separator) {
        try {
            //Set up reader / writer objects to retrieve and place text
            BufferedReader reader = new BufferedReader(new FileReader(JSONLocation));
            BufferedWriter writer = new BufferedWriter(new FileWriter(convertLocation));
            Scanner s = new Scanner(reader);

            //Write first line of .csv file (listing all column header names)
            String firstLine = "";
            for (int i = 0; i < columnNames.length; i++) {
                firstLine += columnNames[i] + separator;
            }
            writer.write(firstLine.substring(0, firstLine.length() - separator.length()) + "\n");

            //Write the data to the .csv file, line-by-line
            long lineCount = 0;
            while(s.hasNextLine()) {
                String currentLine = s.nextLine();
                System.out.println("Extracting information from line " + ++lineCount);
                //Parse the line for all the content
                String csvLine = "";
                for (int i = 0; i < columnNames.length - 1; i++) {  // -1 because last key requires special call to extractJSONValue()'s alternate-argument version
                    csvLine += extractJSONValue(currentLine, columnNames[i], columnNames[i+1]) + separator;
                }
                csvLine += extractLastJSONValue(currentLine, columnNames[columnNames.length - 1]) + "\n";
                //System.out.println(csvLine);
                writer.write(csvLine);
            }

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

    //Extracts value of given key in some JSON text, using nextKey as indicator of where to stop
    //(relies on the formatting used in Yelp's JSON, as some spacing is assumed in this logic here)
    private static String extractJSONValue(String text, String key, String nextKey) {
        //This version does not change any of the content text in any way, using quotes on everything
        /*String value = text.substring(text.indexOf("\"" + key + "\":") + ("\"" + key + "\":").length(),
                text.indexOf(",\"" + nextKey + "\":"));
        if(value.indexOf("\"") != 0) {  // if value is a NOT string, we will add quotes on ends of the value before returning
            return "\"" + value + "\"";  // Any quotes will be escaped by replacing them with TWO quotes
        }
        return value;*/

        //This version does not change any of the content text in any way, not using quotes on anything
        String value = text.substring(text.indexOf("\"" + key + "\":") + ("\"" + key + "\":").length(),
                text.indexOf(",\"" + nextKey + "\":"));
        if(value.indexOf("\"") == 0) {  // if value is a string, we will remove quotes on ends of the value before returning
            return value.substring(1, value.length() - 1);
        }
        return value;

        //This version makes CSVs which have any quotes escaped by using pairs of quotes to escape quotes (works in Excel nicely)
        /*String value = text.substring(text.indexOf("\"" + key + "\":") + ("\"" + key + "\":").length(),
                text.indexOf(",\"" + nextKey + "\":"));
        if(value.indexOf("\"") != 0) {  // if value is a NOT string, we will add quotes on ends of the value before returning
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Any quotes will be escaped by replacing them with TWO quotes
        }
        else {
            value = value.substring(1, value.length() - 1);  // Substring off the quotes on the end of the string
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Substitute any quotes in value with double quotes and put back surrounding quotes
        }*/

        //This version tries to escape commas and quotes in the text with a backslash(\)
        /*String value = text.substring(text.indexOf("\"" + key + "\":") + ("\"" + key + "\":").length(),
                text.indexOf(",\"" + nextKey + "\":"));
        if(value.indexOf("\"") != 0) {  // if value is a NOT string, we will add quotes on ends of the value before returning
            return "\"" + value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",") + "\"";  // Escape quotes / commas with backslash
        }
        else {
            value = value.substring(1, value.length() - 1);  // Substring off the quotes on the end of the string
            return "\"" + value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",") + "\"";  // Escape quotes / commans with backslash, then put back surrounding quotes
        }*/

        //This strategy does not put quotes around any of the data, but still uses an escape character
        /*String value = text.substring(text.indexOf("\"" + key + "\":") + ("\"" + key + "\":").length(),
                text.indexOf(",\"" + nextKey + "\":"));
        if(value.indexOf("\"") != 0) {  // if value is a NOT string, we will add quotes on ends of the value before returning
            return value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",");  // Escape quotes / commas with backslash
        }
        else {
            value = value.substring(1, value.length() - 1);  // Substring off the quotes on the end of the string
            return value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",");  // Escape quotes / commans with backslash, then put back surrounding quotes
        }*/
    }

    //Extracts the value of the last key-value pair of text representing a JSON object
    private static String extractLastJSONValue(String text, String lastKey) {
        //This version does not change any of the content text in any way (works to import to MySQL when using a fancier delimiter)
        /*String value = text.substring(text.indexOf("\"" + lastKey + "\":") + ("\"" + lastKey + "\":").length(),
                text.length()-1);  // -1 to not include ending brace
        if(value.indexOf("\"") != 0) {  // if value is a NOT string, we will add quotes on ends of the value before returning
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Any quotes will be escaped by replacing them with TWO quotes
        }
        return value;*/

        //This version does not change any of the content text in any way, not using quotes on anything
        String value = text.substring(text.indexOf("\"" + lastKey + "\":") + ("\"" + lastKey + "\":").length(),
                text.length()-1);  // -1 to not include ending brace
        if(value.indexOf("\"") == 0) {  // if value is a string, we will remove quotes on ends of the value before returning
            return value.substring(1, value.length() - 1);
        }
        return value;

        //This version makes CSVs which have any quotes escaped by using pairs of quotes to escape quotes (works in Excel nicely)
        /*String value = text.substring(text.indexOf("\"" + lastKey + "\":") + ("\"" + lastKey + "\":").length(),
                text.length()-1);  // -1 to not include ending brace
        if(value.indexOf("\"") != 0) {  // If value is NOT string, we will add quotes on ends of the value before returning
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Any quotes will be escaped by replacing them with TWO quotes
        }
        else {
            value = value.substring(1, value.length() - 1);  // Substring off the quotes on the end of the string
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Substitute any quotes in value with double quotes and put back surrounding quotes
        }*/

        //This version tries to escape commas and quotes in the text with a backslash(\)
        /*String value = text.substring(text.indexOf("\"" + lastKey + "\":") + ("\"" + lastKey + "\":").length(),
                text.length()-1);  // -1 to not include ending brace
        if(value.indexOf("\"") != 0) {  // If value is NOT string, we will add quotes on ends of the value before returning
            return "\"" + value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",") + "\"";  // Escape quotes and commas
        }
        else {
            value = value.substring(1, value.length() - 1);  // Substring off the quotes on the end of the string
            return "\"" + value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",") + "\"";  // Escape quotes and commas
        }*/

        //This strategy does not put quotes around any of the data, but still uses an escape character
        /*String value = text.substring(text.indexOf("\"" + lastKey + "\":") + ("\"" + lastKey + "\":").length(),
                text.length()-1);  // -1 to not include ending brace
        if(value.indexOf("\"") != 0) {  // If value is NOT string, we will add quotes on ends of the value before returning
            return value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",");  // Escape quotes and commas
        }
        else {
            value = value.substring(1, value.length() - 1);  // Substring off the quotes on the end of the string
            return value.replaceAll("\"", ESCAPE_CHAR+"\"").replaceAll(",", ESCAPE_CHAR+",");  // Escape quotes and commas
        }*/
    }
}
