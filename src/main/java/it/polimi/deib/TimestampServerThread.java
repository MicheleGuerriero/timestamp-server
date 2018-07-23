package it.polimi.deib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Created by miik on 09/08/17.
 */
public class TimestampServerThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(TimestampServerThread.class);

    private Socket socket = null;

    private TupleLatencyStore latencyStore = null;

    public TimestampServerThread(Socket socket, TupleLatencyStore latencyStore) {
        super("TimestampServerThread");
        this.socket = socket;
        this.latencyStore = latencyStore;
    }

    public void run() {

        try (
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream()));
        ) {
            String inputLine;

            long startTime = System.currentTimeMillis();

            while ((inputLine = in.readLine()) != null) {

                Long timestamp = System.currentTimeMillis();
                logger.info(inputLine + ", " + timestamp);

                if(inputLine.contains("start")){
                    synchronized (latencyStore){
                        latencyStore.startTuple(inputLine.split("_")[0], timestamp);
                    }
                } else if(inputLine.contains("end")) {
                    synchronized (latencyStore) {
                        latencyStore.endTuple(inputLine.split("_")[0], timestamp);
                    }
                } else if(inputLine.equals("jobStart")) {
                    synchronized (latencyStore) {
                        latencyStore.jobStart();
                    }
                } else if(inputLine.equals("jobEnd")) {
                    synchronized (latencyStore) {
                        latencyStore.jobEnd();
                    }
                }
            }

            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
