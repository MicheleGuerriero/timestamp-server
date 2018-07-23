package it.polimi.deib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimestampServer {

    private static final Logger logger = LoggerFactory.getLogger(TimestampServer.class);


    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            logger.error("Usage: java TimestampServer <port number> <path to latencies file>");
            System.exit(1);
        }

        int portNumber = Integer.parseInt(args[0]);
        String pathToLatenciesFile = args[1];
        TupleLatencyStore latencyStore = new TupleLatencyStore(pathToLatenciesFile);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            while (latencyStore.getListening()) {
                executorService.execute(new TimestampServerThread(serverSocket.accept(), latencyStore));
            }
            executorService.shutdown();
        } catch (IOException e) {
            logger.error("Could not listen on port " + portNumber);
            System.exit(-1);
        }
    }
}
