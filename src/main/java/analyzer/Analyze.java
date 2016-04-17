package main.java.analyzer;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.eclipse.jdt.internal.compiler.codegen.IntegerCache;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by rene on 17-4-16.
 */
public class Analyze {

    private JSONObject charMapping;
    private HashMap<String, Integer> totalCount = new HashMap<>();
    private String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public Analyze(){
        try {
            String jsonData = readFile("/home/rene/output/part-r-00000.json", Charset.defaultCharset());
            this.charMapping = new JSONObject(jsonData);

            for(char c : alphabet.toCharArray()){
                totalCount.put("" + c, createSum(charFrequency(c+"")));

            }

        }catch (Exception ex){
            System.out.print(ex.getMessage());
        }

    }

    public double predict(String word) throws JSONException{
        char prev = '\0';
        double sum = 0;
        for (char x : word.toUpperCase().toCharArray()){
            if (prev == '\0'){
                prev = x;
                continue;
            }else{
                sum += predict(prev, x);
                prev = x;
            }
        }
        sum += predict(prev, ' ');
        double percentage = sum / word.length();


        return percentage;
    }


    private JSONObject charFrequency(String c) throws JSONException{
        return new JSONObject(charMapping.get(c).toString());
    }


    private int createSum(JSONObject object) throws JSONException{
        Iterator<String> keys = object.keys();
        int sum = 0;
        while (keys.hasNext()){
            String key = (String)keys.next();
            sum += Integer.parseInt(object.get(key).toString());
        }
        return sum;
    }

    private double predict(char x, char y) throws JSONException{
        JSONObject obj = charFrequency(x + "");
        double value = valueAt(obj, y);
        return value / totalCount.get(x + "") * 100;
    }

    private int valueAt(JSONObject object, char x){
        int value = 0;
        try {
            value = Integer.parseInt(object.get(x + "").toString());
        }catch(Exception ex){
        }
        return value;
    }

    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

}
