package com.zdp.stream.consumer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JavaProcess {

    public static void main(final String[] args) throws IOException, InterruptedException {
        //Build command 
        String command="/opt/spark-1.3.1/bin/spark-submit --class StreamConsumer --master local[*]  /home/19491/stream-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar";
        //Add arguments
        //commands.add("/home/narek/pk.txt");
        System.out.println(command);

        //Run macro on target
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "/home/19491/consumer.sh");
        //pb.directory(new File("/home/19491/"));
        pb.redirectErrorStream(true);
        Process process = pb.start();

        //Read output
        StringBuilder out = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null, previous = null;
        while ((line = br.readLine()) != null)
            if (!line.equals(previous)) {
                previous = line;
                out.append(line).append('\n');
                System.out.println(line);
            }

        //Check result
        if (process.waitFor() == 0) {
            System.out.println("Success!");
            System.exit(0);
        }

        //Abnormal termination: Log command parameters and output and throw ExecutionException
        System.err.println(command);
        System.err.println(out.toString());
        System.exit(1);
    }
}