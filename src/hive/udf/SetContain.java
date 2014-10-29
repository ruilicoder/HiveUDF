package hive.udf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

//Check whether a line is in a file

public class SetContain extends UDF {
    private Set<String> set;

    public boolean evaluate(String key, String mapFile) throws HiveException {
        if (set == null) {
            set = new HashSet<String>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));

                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    //String[] pair = line.split("\t");
                    //String k = pair[0];
                    //String v= pair[1];
                    set.add(line);
                }
            } catch (FileNotFoundException e) {
                throw new HiveException(mapFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + mapFile + " failed, please check format");
            }
        }

        if (set.contains(key)) {
            return true;
        }

        return false;
    }
}
