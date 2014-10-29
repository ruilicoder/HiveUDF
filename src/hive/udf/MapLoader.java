package hive.udf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapContains extends UDF {
    private Map<String, String> map;

    public String evaluate(String key, String mapFile) throws HiveException {
        if (map == null) {
            map = new HashMap<String, String>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));

                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    String[] pair = line.split("\t");
                    String k = pair[0];
                    String v= pair[1];
                    map.put(k, v);
                }
            } catch (FileNotFoundException e) {
                throw new HiveException(mapFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + mapFile + " failed, please check format");
            }
        }

        if (map.containsKey(key)) {
            return map.get(key);
        }

        return null;
    }
}