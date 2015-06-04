package mydatabase;

import com.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shraddha
 */
public class MyDatabase {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Please select one of the following:");
        System.out.println("1. Parse the input CSV file to binary");
        System.out.println("2. Choose a field to query");
        System.out.println("3. Exit");
        
        Scanner input = new Scanner(System.in);
        int option;
        option = input.nextInt();

        while(option != 3){
            if(option == 0)
		{
			System.out.println();
			System.out.println();
			System.out.println("Please select one of the following:");
                        System.out.println("1. Parse the input CSV file to binary");
                        System.out.println("2. Choose a field to query");
                        System.out.println("3. Exit");
                        option = input.nextInt();
			input.nextLine();
		}
            if (option == 1) {
                parseCSVToBinary();
                option = 0;
            } else if (option == 2) {
                queryField();
                option = 0;
            }
        }
    }

    public static void parseCSVToBinary() throws IOException {
        CSVReader reader = null;
        byte bool[] = new byte[4];
        byte booleanValues;
        DataOutputStream idIndexOut = null;
        DataOutputStream companyIndexOut = null;
        DataOutputStream drugIndexOut = null;
        DataOutputStream trialsIndexOut = null;
        DataOutputStream patientsIndexOut = null;
        DataOutputStream dosageIndexOut = null;
        DataOutputStream readingIndexOut = null;
        DataOutputStream doubleBlindIndexOut = null;
        DataOutputStream csIndexOut = null;
        DataOutputStream gfIndexOut = null;
        DataOutputStream fdaIndexOut = null;

        Map<Integer, Integer> idIndexMap = new TreeMap<Integer, Integer>();
        Map<String, String> companyIndexMap = new TreeMap<String, String>();
        Map<String, String> drugIndexMap = new TreeMap<String, String>();
        Map<Integer, String> trialsIndexMap = new TreeMap<Integer, String>();
        Map<Integer, String> patientsIndexMap = new TreeMap<Integer, String>();
        Map<Integer, String> dosageIndexMap = new TreeMap<Integer, String>();
        Map<Float, String> readingIndexMap = new TreeMap<Float, String>();

        Map<String, String> doubleBlindIndexMap = new TreeMap<String, String>();
        Map<String, String> csIndexMap = new TreeMap<String, String>();
        Map<String, String> gfIndexMap = new TreeMap<String, String>();
        Map<String, String> fdaIndexMap = new TreeMap<String, String>();

        try {
            //the path needs to be changed according to where the file is stored
            reader = new CSVReader(new FileReader(new File("C:\\Users\\Shraddha\\Documents\\NetBeansProjects\\MyDatabase\\PHARMA_TRIALS_1000B.csv")));
            String[] data;
            int size = 0;
            RandomAccessFile file = new RandomAccessFile("data.db", "rw");
            idIndexOut = new DataOutputStream(new FileOutputStream("id.ndx"));
            companyIndexOut = new DataOutputStream(new FileOutputStream("company.ndx"));
            drugIndexOut = new DataOutputStream(new FileOutputStream("drug.ndx"));
            trialsIndexOut = new DataOutputStream(new FileOutputStream("trials.ndx"));
            patientsIndexOut = new DataOutputStream(new FileOutputStream("patients.ndx"));
            dosageIndexOut = new DataOutputStream(new FileOutputStream("dosage.ndx"));
            readingIndexOut = new DataOutputStream(new FileOutputStream("reading.ndx"));
            doubleBlindIndexOut = new DataOutputStream(new FileOutputStream("doubleBlind.ndx"));
            csIndexOut = new DataOutputStream(new FileOutputStream("controlledStudy.ndx"));
            gfIndexOut = new DataOutputStream(new FileOutputStream("govtFunded.ndx"));
            fdaIndexOut = new DataOutputStream(new FileOutputStream("fdaApproved.ndx"));
            char[] company, drugId;
            reader.readNext();
            
            while ((data = reader.readNext()) != null) {
                int id = Integer.parseInt(data[0]);
                if (idIndexMap.get(id) == null) {
                    idIndexMap.put(id, size);
                }
                file.writeInt(id);
                size += 4;
                if (companyIndexMap.get(data[1]) == null) {
                    companyIndexMap.put(data[1], idIndexMap.get(id) + "");
                } else {
                    companyIndexMap.put(data[1], companyIndexMap.get(data[1]) + "," + idIndexMap.get(id) + "");
                }
                
                file.writeByte(data[1].length());
                size += 1;
                company = data[1].toCharArray();
                for (int i = 0; i < company.length; i++) {
                    file.writeByte(company[i]);
                }
                size += data[1].length();
                drugId = data[2].toCharArray();
                if (drugIndexMap.get(data[2]) == null) {
                    drugIndexMap.put(data[2], idIndexMap.get(id) + "");
                } else {
                    drugIndexMap.put(data[2], drugIndexMap.get(data[2]) + "," + idIndexMap.get(id) + "");
                }

                size += 6;
                for (int i = 0; i < drugId.length; i++) {
                    file.writeByte(drugId[i]);
                }
                file.writeShort(Short.parseShort(data[3]));
                size += 2;

                if (trialsIndexMap.get(Integer.parseInt(data[3])) == null) {
                    trialsIndexMap.put(Integer.parseInt(data[3]), idIndexMap.get(id) + "");
                } else {
                    trialsIndexMap.put(Integer.parseInt(data[3]), trialsIndexMap.get(Integer.parseInt(data[3])) + "," + idIndexMap.get(id) + "");
                }
                file.writeShort(Short.parseShort(data[4]));
                size += 2;
                if (patientsIndexMap.get(Integer.parseInt(data[4])) == null) {
                    patientsIndexMap.put(Integer.parseInt(data[4]), idIndexMap.get(id) + "");
                } else {
                    patientsIndexMap.put(Integer.parseInt(data[4]), patientsIndexMap.get(Integer.parseInt(data[4])) + "," + idIndexMap.get(id) + "");
                }
                file.writeShort(Short.parseShort(data[5]));
                size += 2;
                if (dosageIndexMap.get(Integer.parseInt(data[5])) == null) {
                    dosageIndexMap.put(Integer.parseInt(data[5]), idIndexMap.get(id) + "");
                } else {
                    dosageIndexMap.put(Integer.parseInt(data[5]), dosageIndexMap.get(Integer.parseInt(data[5])) + "," + idIndexMap.get(id) + "");
                }
                file.writeFloat(Float.parseFloat(data[6]));
                if (readingIndexMap.get(Float.parseFloat(data[6])) == null) {
                    readingIndexMap.put(Float.parseFloat(data[6]), idIndexMap.get(id) + "");
                } else {
                    readingIndexMap.put(Float.parseFloat(data[6]), readingIndexMap.get(Float.parseFloat(data[6])) + "," + idIndexMap.get(id) + "");
                }

                size += 4;
                if (data[7].equalsIgnoreCase("true")) {
                    bool[0] = (byte) 0x08;
                } else {
                    bool[0] = (byte) 0x00;
                }
                if (doubleBlindIndexMap.get(data[7]) == null) {
                    doubleBlindIndexMap.put(data[7], idIndexMap.get(id) + "");
                } else {
                    doubleBlindIndexMap.put(data[7], doubleBlindIndexMap.get(data[7]) + "," + idIndexMap.get(id) + "");
                }
                if (data[8].equalsIgnoreCase("true")) {
                    bool[1] = (byte) 0x04;
                } else {
                    bool[1] = (byte) 0x00;
                }
                if (csIndexMap.get(data[8]) == null) {
                    csIndexMap.put(data[8], idIndexMap.get(id) + "");
                } else {
                    csIndexMap.put(data[8], csIndexMap.get(data[8]) + "," + idIndexMap.get(id) + "");
                }
                if (data[9].equalsIgnoreCase("true")) {
                    bool[2] = (byte) 0x02;
                } else {
                    bool[2] = (byte) 0x00;
                }
                if (gfIndexMap.get(data[9]) == null) {
                    gfIndexMap.put(data[9], idIndexMap.get(id) + "");
                } else {
                    gfIndexMap.put(data[9], gfIndexMap.get(data[9]) + "," + idIndexMap.get(id) + "");
                }
                if (data[10].equalsIgnoreCase("true")) {
                    bool[3] = (byte) 0x01;
                } else {
                    bool[3] = (byte) 0x00;
                }
                if (fdaIndexMap.get(data[10]) == null) {
                    fdaIndexMap.put(data[10], idIndexMap.get(id) + "");
                } else {
                    fdaIndexMap.put(data[10], fdaIndexMap.get(data[10]) + "," + idIndexMap.get(id) + "");
                }
                booleanValues = (byte) (bool[0] | bool[1] | bool[2] | bool[3]);
                file.writeByte(booleanValues);
                size += 1;
            }
            Iterator it = idIndexMap.entrySet().iterator();
            StringBuffer buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "," + pair.getValue());
                buff.append("\n");
            }

            idIndexOut.writeBytes(buff.toString());

            it = companyIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            companyIndexOut.writeBytes(buff.toString());

            it = drugIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            drugIndexOut.writeBytes(buff.toString());

            it = trialsIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            trialsIndexOut.writeBytes(buff.toString());

            it = patientsIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            patientsIndexOut.writeBytes(buff.toString());

            it = dosageIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            dosageIndexOut.writeBytes(buff.toString());

            it = readingIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            readingIndexOut.writeBytes(buff.toString());

            it = doubleBlindIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            doubleBlindIndexOut.writeBytes(buff.toString());

            it = csIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            csIndexOut.writeBytes(buff.toString());

            it = gfIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            gfIndexOut.writeBytes(buff.toString());

            it = fdaIndexMap.entrySet().iterator();
            buff = new StringBuffer();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                buff.append(pair.getKey() + "=" + pair.getValue());
                buff.append("\n");

            }

            fdaIndexOut.writeBytes(buff.toString());
            System.out.println("Binary File Created!!");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void queryUsingID(int id) throws Exception {
        
        String filePath = "id.ndx";
        try {
            RandomAccessFile file = new RandomAccessFile("data.db", "r");
            FileInputStream fs = new FileInputStream(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs));
            for (int i = 0; i < id - 1; ++i) {
                br.readLine();
            }
            String readLine = br.readLine();
            String[] arr = readLine.split(",");
            int bytesToSkip = Integer.parseInt(arr[1]);
            file.seek(bytesToSkip);
            int a = file.readInt();
            int size = file.read();
            System.out.print(a);
            System.out.print("  ");

            byte[] companyNames = new byte[size];
            for (int i = 0; i < size; i++) {
                companyNames[i] = (byte) file.read();
            }
            String companyName = new String(companyNames);
            System.out.print(companyName);
            for (int i = 0; i < 45 - size; i++) {
                System.out.print(" ");
            }

            byte[] drugId = new byte[6];
            for (int i = 0; i < 6; i++) {
                drugId[i] = (byte) file.read();
            }
            String drugName = new String(drugId);
            System.out.print(drugName);
            System.out.print("   ");

            int trials = file.readShort();
            int patients = file.readShort();
            int dosage = file.readShort();
            float reading = file.readFloat();
            System.out.print(trials);
            System.out.print("      ");
            System.out.print(patients);
            System.out.print("      ");
            System.out.print(dosage);
            System.out.print("      ");
            System.out.print(reading);
            System.out.print("      ");

            byte commonByte = file.readByte();
            if ((commonByte & 0x08) == 0) {
                System.out.print("FALSE");
                System.out.print("        ");
            } else {
                System.out.print("TRUE");
                System.out.print("         ");
            }

            if ((commonByte & 0x04) == 0) {
                System.out.print("FALSE");
                System.out.print("             ");
            } else {
                System.out.print("TRUE");
                System.out.print("              ");
            }

            if ((commonByte & 0x02) == 0) {
                System.out.print("FALSE");
                System.out.print("       ");
            } else {
                System.out.print("TRUE");
                System.out.print("        ");
            }

            if ((commonByte & 0x01) == 0) {
                System.out.print("FALSE");
            } else {
                System.out.print("TRUE");
            }
            System.out.println("");
        } catch (IOException | NumberFormatException e) {
            System.out.println("Exception!!" + e);
        }
    }

    public static void queryByCompanyName(String companyName) throws Exception {
        
        FileInputStream fs = null;
        BufferedReader br = null;
        companyName = companyName.trim();
        try {
            String filePath = "company.ndx";
            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (companyName.equalsIgnoreCase(nextLineArr[0])) {
                    reqdLine = nextLine;
                    break;
                }
            }

            String[] data = reqdLine.split("=");

            String[] id = data[1].split(",");
            printData(id);

       } finally {
            fs.close();
            br.close();
        }
    }

    public static void queryByDrugName(String drugName) throws Exception {
        
        FileInputStream fs = null;
        BufferedReader br = null;
        drugName = drugName.trim();
        try {
            String filePath = "drug.ndx";
            
            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (drugName.equalsIgnoreCase(nextLineArr[0])) {
                    reqdLine = nextLine;
                    break;
                }
            }

            String[] data = reqdLine.split("=");

            String[] id = data[1].split(",");

            printData(id);

        } finally {
            
            fs.close();
            br.close();
        }

    }

    public static void queryByTrials(int trials) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "trials.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (trials == Integer.parseInt(nextLineArr[0])) {
                    reqdLine = nextLine;
                    break;
                }
            }

            String[] data = reqdLine.split("=");

            String[] id = data[1].split(",");

            printData(id);

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryByPatients(int patients) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "patients.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (patients == Integer.parseInt(nextLineArr[0])) {
                    reqdLine = nextLine;
                    break;
                }
            }

            if (reqdLine != null && !("".equals(reqdLine))) {
                String[] data = reqdLine.split("=");

                String[] id = data[1].split(",");

                printData(id);
            }

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryByDosage(int dosage) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "dosage.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (dosage == Integer.parseInt(nextLineArr[0])) {
                    reqdLine = nextLine;
                    break;
                }
            }

            if (reqdLine != null && !("".equals(reqdLine))) {
                String[] data = reqdLine.split("=");

                String[] id = data[1].split(",");

                printData(id);
            }

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryByReading(float reading, String operator) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;
        String[] id = null;
	boolean flag = false;

        try {
            String filePath = "reading.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (operator.equals("=")) {
                    if (reading == Float.parseFloat(nextLineArr[0])) {
                        reqdLine = nextLine;

                    }

                    if (reqdLine != null && !("".equals(reqdLine))) {
                        String[] data = reqdLine.split("=");

                        id = data[1].split(",");
                        
                        if(id!=null && id.length>0)
                            printData(id);
                        
                        id=null;
                        reqdLine=null;
                        flag = true;
                    }

                } else if (operator.equals(">")) {

                    if (Float.parseFloat(nextLineArr[0]) > reading) {
                        reqdLine = nextLine;

                    }

                    if (reqdLine != null && !("".equals(reqdLine))) {
                        String[] data = reqdLine.split("=");

                         id = data[1].split(",");
                         if(id!=null && id.length>0)
                            printData(id);
                        
                        id=null;
                        reqdLine=null;
                        
                    }

                } else if (operator.equals(">=")) {

                    if (Float.parseFloat(nextLineArr[0]) >= reading) {
                        reqdLine = nextLine;

                    }

                    if (reqdLine != null && !("".equals(reqdLine))) {
                        String[] data = reqdLine.split("=");

                         id = data[1].split(",");
                         if(id!=null && id.length>0)
                            printData(id);
                        
                        id=null;
                        reqdLine=null;
                        
                    }

                } else if (operator.equals("<")) {

                    if (Float.parseFloat(nextLineArr[0]) < reading) {
                        reqdLine = nextLine;

                    }

                    if (reqdLine != null && !("".equals(reqdLine))) {
                        String[] data = reqdLine.split("=");

                         id = data[1].split(",");
                          if(id!=null && id.length>0)
                            printData(id);
                        
                        id=null;
                        reqdLine=null;
                        
                    }

                } else if (operator.equals("<=")) {

                    if (Float.parseFloat(nextLineArr[0]) <= reading) {
                        reqdLine = nextLine;

                    }

                    if (reqdLine != null && !("".equals(reqdLine))) {
                        String[] data = reqdLine.split("=");

                         id = data[1].split(",");
                         if(id!=null && id.length>0)
                            printData(id);
                        
                        id=null;
                        reqdLine=null;
                        
                    }

                } else if (operator.equals("!=")) {

                    if (Float.parseFloat(nextLineArr[0]) != reading) {
                        reqdLine = nextLine;

                    }

                    if (reqdLine != null && !("".equals(reqdLine))) {
                        String[] data = reqdLine.split("=");

                         id = data[1].split(",");
                         if(id!=null && id.length>0)
                            printData(id);
                        
                        id=null;
                        reqdLine=null;
                        
                    }

                }
                if(flag)
                    break;
            }

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryByDoubleBlind(String tf) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "doubleBlind.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (tf.equalsIgnoreCase(nextLineArr[0])) {
                    reqdLine = nextLine;

                }
            }

            if (reqdLine != null && !("".equals(reqdLine))) {
                String[] data = reqdLine.split("=");

                String[] id = data[1].split(",");

                printData(id);
            }

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryByControlledStudy(String tf) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "controlledStudy.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (tf.equalsIgnoreCase(nextLineArr[0])) {
                    reqdLine = nextLine;

                }
            }

            if (reqdLine != null && !("".equals(reqdLine))) {
                String[] data = reqdLine.split("=");

                String[] id = data[1].split(",");

                printData(id);
            }

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryBygovtFunded(String tf) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "govtFunded.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (tf.equalsIgnoreCase(nextLineArr[0])) {
                    reqdLine = nextLine;

                }
            }

            if (reqdLine != null && !("".equals(reqdLine))) {
                String[] data = reqdLine.split("=");

                String[] id = data[1].split(",");

                printData(id);
            }

        } finally {

            fs.close();
            br.close();
        }

    }

    public static void queryByFDAApproved(String tf) throws Exception {

        FileInputStream fs = null;
        BufferedReader br = null;

        try {
            String filePath = "fdaApproved.ndx";

            fs = new FileInputStream(filePath);
            br = new BufferedReader(new InputStreamReader(fs));

            String reqdLine = "";
            String nextLine = "";

            while ((nextLine = br.readLine()) != null) {

                String nextLineArr[] = nextLine.split("=");
                if (tf.equalsIgnoreCase(nextLineArr[0])) {
                    reqdLine = nextLine;

                }
            }

            if (reqdLine != null && !("".equals(reqdLine))) {
                String[] data = reqdLine.split("=");

                String[] id = data[1].split(",");

                printData(id);
            }

        } finally {

            fs.close();
            br.close();
        }

    }
    public static void queryField() throws IOException {
        Scanner input = new Scanner(System.in);
        File dbfile = new File("data.db");
            if (!dbfile.exists()) {
                System.out.println("Data file does not exist!!");
                System.out.println("Please create the data file and run the program again!");
                System.out.println("Select one of the following:");
                System.out.println("1. Parse the input CSV file to binary ");
                System.out.println("2. Exit");
                int selection = input.nextInt();
                if (selection == 1) {
                    parseCSVToBinary();
                } else {
                    return;
                }
            } else {
                System.out.println("Please select a field to query from the following:");
                System.out.println("1. ID");
                System.out.println("2. Company");
                System.out.println("3. Drug ID");
                System.out.println("4. Trials");
                System.out.println("5. Patients");
                System.out.println("6. Dosage");
                System.out.println("7. Reading");
                System.out.println("8. Double Blind");
                System.out.println("9. Controlled Study");
                System.out.println("10. Government Funded");
                System.out.println("11. FDA Approved");

                int field = input.nextInt();
                if (field == 1 || (field > 3 && field <= 7)) {
                    System.out.println("Please select one operator from the following :");
                    System.out.println(">,<,>=,<=,=,!=");
                    String operator = input.next();
                    operator = operator.trim();
                    System.out.println("Please enter a value");
                    String queryVal = input.next();
                    if (field == 1) {
                        try {
                            System.out.println("ID   Company   Drug-Id  Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            int queryValue = Integer.parseInt(queryVal);
                            switch (operator) {
                                case "=":
                                    queryUsingID(queryValue);
                                    break;
                                case ">":
                                    for (int i = queryValue + 1; i <= 1000; i++) {
                                        queryUsingID(i);
                                    }
                                    break;
                                case ">=":
                                    for (int i = queryValue; i <= 1000; i++) {
                                        queryUsingID(i);
                                    }
                                    break;
                                case "<":
                                    for (int i = 1; i < queryValue; i++) {
                                        queryUsingID(i);
                                    }
                                    break;
                                case "<=":
                                    for (int i = 1; i <= queryValue; i++) {
                                        queryUsingID(i);
                                    }
                                    break;
                                case "!=":
                                    for (int i = 1; i <= 1000 ; i++) {
                                        if (i == queryValue)
                                            continue;
                                        else
                                            queryUsingID(i);
                                    }
                                    break;
                                default:
                                    System.out.println("Invalid choice");
                            }

                        }catch(NullPointerException n)
			{
                            System.out.println("The queried id does not exist in data file");
			} 
                        catch (Exception e) {
                            System.out.println("Exception.." + e);
                        }
                    } else if (field == 4) {
                        try {
                            System.out.println("ID   Company                                      Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            int queryValue = Integer.parseInt(queryVal);
                            switch (operator) {
                                case "=":
                                    queryByTrials(queryValue);
                                    break;
                                case ">":
                                    for (int i = queryValue + 1; i <= 100; i++) {
                                        queryByTrials(i);
                                    }
                                    break;
                                case ">=":
                                    for (int i = queryValue; i <= 100; i++) {
                                        queryByTrials(i);
                                    }
                                    break;
                                case "<":
                                    for (int i = 10; i < queryValue; i++) {
                                        queryByTrials(i);
                                    }
                                    break;
                                case "<=":
                                    for (int i = 10; i <= queryValue; i++) {
                                        queryByTrials(i);
                                    }
                                    break;
                                case "!=":
                                    for (int i = 10; i <= 100 ; i++) {
                                        if (i == queryValue)
                                            continue;
                                        else
                                            queryByTrials(i);
                                    }
                                    break;
                                default:
                                    System.out.println("Invalid choice");
                            }
                        }catch(NullPointerException n)
			{
				System.out.println("The queried Number of trials does not exist in data file");
			} 
                        catch (Exception e) {
                            System.out.println("Exception.." + e);
                        }
                    } else if (field == 5) {
                        try {
                            System.out.println("ID   Company                                   Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            int queryValue = Integer.parseInt(queryVal);
                            switch (operator) {
                                case "=":
                                    queryByPatients(queryValue);
                                    break;
                                case ">":
                                    for (int i = queryValue + 1; i <= 2500; i++) {
                                        queryByPatients(i);
                                    }
                                    break;
                                case ">=":
                                    for (int i = queryValue; i <= 2500; i++) {
                                        queryByPatients(i);
                                    }
                                    break;
                                case "<":
                                    for (int i = 1000; i < queryValue; i++) {
                                        queryByPatients(i);
                                    }
                                    break;
                                case "<=":
                                    for (int i = 1000; i <= queryValue; i++) {
                                        queryByPatients(i);
                                    }
                                    break;
                                case "!=":
                                    for (int i = 1000; i <= 2500 ; i++) {
                                        if (i == queryValue)
                                            continue;
                                        else
                                            queryByPatients(i);
                                    }
                                    break;
                                default:
                                    System.out.println("Invalid choice..!!");
                            }
                        }catch(NullPointerException n)
			{
                            System.out.println("The queried number of Patients does not exist in data file");
			} 
                        catch (Exception e) {
                            System.out.println("Exception.." + e);
                        }
                    } else if (field == 6) {
                        try {
                            System.out.println("ID   Company                                      Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            int queryValue = Integer.parseInt(queryVal);
                            switch (operator) {
                                case "=":
                                    queryByDosage(queryValue);
                                    break;
                                case ">":
                                    for (int i = queryValue + 1; i <= 500; i++) {
                                        queryByDosage(i);
                                    }
                                    break;
                                case ">=":
                                    for (int i = queryValue; i <= 500; i++) {
                                        queryByDosage(i);
                                    }
                                    break;
                                case "<":
                                    for (int i = 6; i < queryValue; i++) {
                                        queryByDosage(i);
                                    }
                                    break;
                                case "<=":
                                    for (int i = 6; i <= queryValue; i++) {
                                        queryByDosage(i);
                                    }
                                    break;
                                case "!=":
                                    for (int i = 6; i <= 500 ; i++) {
                                        if (i == queryValue)
                                            continue;
                                        else
                                            queryByDosage(i);
                                    }
                                    break;
                                default:
                                    System.out.println("Invalid choice");
                            }

                        }catch(NullPointerException n)
                        {
                            System.out.println("The queried dosage does not exist in data file");
			} 
                        catch (Exception e) {
                            System.out.println("Exception..." + e);
                        }

                    } else if (field == 7) {

                        try {
                            System.out.println("ID   Company                                      Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            float queryValue = Float.parseFloat(queryVal);

                            queryByReading(queryValue, operator);

                        }catch(NullPointerException n)
			{
                            System.out.println("The queried reading value does not exist in data file");
                        } 
                        catch (Exception e) {
                            System.out.println("Exception..." + e);
                        }
                    }

                } else if (field == 2) {

                    System.out.println("Please enter a Company name to query for :");
                    String companyName = null;

                    companyName = input.next();
                    companyName = companyName + input.nextLine();
                    
                    try {
                        if (companyName != null && !companyName.equals("")) {
                            System.out.println("ID   Company  Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            queryByCompanyName(companyName);
                        }
                    }
                    catch(ArrayIndexOutOfBoundsException a)
                    {
                        System.out.println("The queried company name does not exist in data file");
			}
                    catch (Exception e) {
                        System.out.println("Exception.." + e);
                    }

                } else if (field == 3) {

                    System.out.println("Please enter a drug name to query for : ");
                    String drugName = null;

                    drugName = input.nextLine();
                    drugName = drugName + input.nextLine();
                    try {
                        if (drugName != null && !drugName.equals("")) {
                            System.out.println("ID   Company                                      Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                            queryByDrugName(drugName);
                        }
                    }catch(ArrayIndexOutOfBoundsException a)
                    {
			System.out.println("The queried drug id does not exist in data file");
                    } 
                    catch (Exception e) {
                        System.out.println("Please create binary database and index files before querying data" + e);
                    }

                } else {

                    System.out.println("Please enter either true or false");
                    String tf = input.next();
                    input.nextLine();

                    if (field == 8) {

                        try {
                            if (tf != null) {
                                System.out.println("ID   Company                                      Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                                queryByDoubleBlind(tf);
                            }
                        } catch (Exception e) {
                            System.out.println("Please create binary database and index files before querying data" + e);
                        }

                    } else if (field == 9) {

                        try {
                            if (tf != null) {
                                System.out.println("ID   Company        Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                                queryByControlledStudy(tf);
                            }
                        } catch (Exception e) {
                            System.out.println("Please create binary database and index files before querying data" + e);
                        }

                    } else if (field == 10) {

                        try {
                            if (tf != null) {
                                System.out.println("ID   Company   Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                                queryBygovtFunded(tf);
                            }
                        } catch (Exception e) {
                            System.out.println(" " + e);
                        }

                    } else if (field == 11) {

                        try {
                            if (tf != null) {
                                System.out.println("ID   Company  Drug-Id Trials  Patients  Dosage  Reading  Double-Blind Controlled-Study Govt-Funded FDA-Approved");
                                queryByFDAApproved(tf);
                            }
                        } catch (Exception e) {
                            System.out.println(" " + e);
                        }
                    }
                }
            }
    }
    public static void printData(String[] arr) throws Exception
	{

		RandomAccessFile file = null;

		try{
			file = new RandomAccessFile("data.db", "r");
			for (int k=0;k<arr.length;k++)
			{
				int bytesToSkip = Integer.parseInt(arr[k]);

				file.seek(bytesToSkip);
				int a=file.readInt();
				int size = file.read();
				System.out.print(a);
				System.out.print("  ");

				byte[] companyNames = new byte[size];
				for(int i=0;i<size;i++)
					companyNames[i]=(byte) file.read();


                                String compName = new String(companyNames);
				System.out.print(compName);
				System.out.print(" ");
				
                                byte[] drugId = new byte[6];
				for(int i=0;i<6;i++)
					drugId[i]=(byte) file.read();

				String drugName = new String(drugId);

				System.out.print(drugName);
				System.out.print("   ");

				int trials = file.readShort();

				int patients = file.readShort();

				int dosage = file.readShort();

				float reading = file.readFloat();
				System.out.print(trials);
				System.out.print("      ");
				System.out.print(patients);
				System.out.print("      ");
				System.out.print(dosage);
				System.out.print("      ");
				System.out.print(reading);
				System.out.print("      ");
				byte commonByte = file.readByte();

				if((commonByte & 0x08)==0)
				{
					System.out.print("FALSE");
					System.out.print("        ");
				}
				else
				{
					System.out.print("TRUE");
					System.out.print("         ");

				}

				if((commonByte & 0x04)==0)
				{
					System.out.print("FALSE");
					System.out.print("             ");

				}
				else
				{
					System.out.print("TRUE");
					System.out.print("              ");

				}

				if((commonByte & 0x02)==0)
				{
					System.out.print("FALSE");
					System.out.print("       ");
				}
				else
				{
					System.out.print("TRUE");
					System.out.print("        ");
				}

				if((commonByte & 0x01)==0){

					System.out.print("FALSE");
					System.out.print("     ");
				}

				else{
					System.out.print("TRUE");
					System.out.print("      ");
				}

                                System.out.println("");
			}
		}
		finally{
			file.close();
                        
		}
	}
}
