package it.polimi.deib;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by miik on 10/08/17.
 */
public class TupleLatencyStore {

    private Map<Integer, Long> flowingTuples;

    private Map<Integer, Long> tuplesLatencies;

    private Boolean listening;

    private BufferedWriter writer;

    public Boolean getListening() {
        return listening;
    }

    public void setListening(Boolean listening) {
        this.listening = listening;
    }

    public TupleLatencyStore(String pathToLatenciesFile){

        flowingTuples = new HashMap<Integer, Long>();
        tuplesLatencies = new HashMap<Integer, Long>();

        listening = true;

        try {
            File latenciesFile = new File(pathToLatenciesFile);

            writer = new BufferedWriter(new FileWriter(latenciesFile));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Boolean startTuple(String tId, Long startTimestamp){
        Integer tSeqIndex = Integer.parseInt(tId.substring(1, tId.length()));

        if(!flowingTuples.containsKey(tSeqIndex)){
            flowingTuples.put(tSeqIndex, startTimestamp);
            return true;
        } else {
            return false;
        }
    }

    public Boolean endTuple(String tId, Long endTimestamp){
        Integer tSeqIndex = Integer.parseInt(tId.substring(1, tId.length()));

        if(!flowingTuples.containsKey(tSeqIndex)){
            return false;
        } else {
            Iterator<Map.Entry<Integer, Long>> iter = flowingTuples.entrySet().iterator();

            while(iter.hasNext()){
                Map.Entry<Integer, Long> t = iter.next();
                if(t.getKey()<=tSeqIndex){
                    Long latency = endTimestamp-t.getValue();
                    try {
                        writer.write(t.getKey() + "," + latency + "\n");
                        tuplesLatencies.put(t.getKey(), latency);
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    iter.remove();
                }
            }
            return true;
        }
    }

    public void jobStart() {
        try {
            flowingTuples.clear();
            writer.flush();


            if(tuplesLatencies.size() > 0) {
                Long totLatency = new Long(0);

                for(Integer t: tuplesLatencies.keySet()){
                    totLatency = totLatency + tuplesLatencies.get(t);
                }
                writer.write("AVG_LATENCY," + totLatency/tuplesLatencies.size() + "\n");
                tuplesLatencies.clear();
            }

            writer.write("NEWJOB," + System.currentTimeMillis() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void jobEnd() {
        try {

            Long totLatency = new Long(0);

            for(Integer t: tuplesLatencies.keySet()){
                totLatency = totLatency + tuplesLatencies.get(t);
            }
            writer.write("AVG_LATENCY," + totLatency/tuplesLatencies.size() + "\n");
            tuplesLatencies.clear();
            this.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
