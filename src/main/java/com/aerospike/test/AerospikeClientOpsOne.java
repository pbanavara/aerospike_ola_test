package com.aerospike.test;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class AerospikeClientOpsOne {
    private AerospikeClient client;
    final ArrayList<Key> keys = new ArrayList<Key>();
    public AerospikeClientOpsOne() {
        System.out.println("Class initialized");
        try {
            client = new AerospikeClient("0.0.0.0", 3000);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String saveRecord(String value1, String value2, String keyname) {

        try {
            //client = new AerospikeClient("0.0.0.0", 3000);
            Key key = new Key("test", "demo", keyname);
            Bin bin1 = new Bin("bin1", value1);
            Bin bin2 = new Bin("bin2", value2);
            //client.delete(null, key);
            client.put(null, key, bin1, bin2);
            return "Done";

        } catch(Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }

    public void scanRecords() {
        ScanPolicy policy = new ScanPolicy();
        policy.concurrentNodes = true;
        policy.includeBinData = true;
        client.scanAll(policy, "test", "demo", new ScanCallback() {

            @Override
            public void scanCallback(Key key, Record record)
                    throws AerospikeException {
                keys.add(key);

            }
        }, "bin1");
    }

    public void saveMapRecord(Key key) {
        try {
            HashMap<String, List> map = new HashMap<String, List>();
            int[] ints = {1,2,4,3};
            map.put("profile_id_1", Arrays.asList(ints));
            Bin bin1 = new Bin("bin1", map);
            client.put(null, key, bin1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        client.close();
    }

    public String readRecord(String keyname) {
        Key key = new Key("test", "demo", keyname);
        Record rec = client.get(null, key);
        return rec.bins.get("bin2").toString();
    }

    public static void main(String[] args) {
       AerospikeClientOpsOne ops = new AerospikeClientOpsOne();
       int NUM_ITERATIONS = 1000;
//       for (int i = 0; i< NUM_ITERATIONS; ++i) {
//           ops.saveRecord("this is a fit for nothing blah record types a hundred one dummy morons trying to fit something to their schedule",
//                   "aakjsdhfkajshdfkajhsdfkjasldkfjal;sdkfj a laskjdfl;kajsd;flkawrwer aldskfja;lskdjf;alskdfj", "testkey");
//           ops.readRecord("testkey");
//           ops.close();
//       }

    }

}
