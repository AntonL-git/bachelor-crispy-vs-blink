package org.example;


import java.io.*;

public class SimpleApp {
    public static void main(String[] args) {
        //select specific sampler

        MLSampler.runSampling(0.005, 5);
        //GraphSampler.runSampling(0.005, 5);
    }

    /*
      Change path of input data for HiBench
   */
    public static void setInputPath(String filePath, String key, String value){
        StringBuilder modifiedContent = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(key)) {
                    line = key + " " + value;
                }
                modifiedContent.append(line).append(System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to load properties");
            return;
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(modifiedContent.toString());
            System.out.println("Configuration updated successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}