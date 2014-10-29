package hive.udf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class MergesKeysToJson extends GenericUDF {
	
	
	public HashMap<String,Double> convertToMap(String str) {
		//if (!str.matches("\\{.*\\}")) {return new HashMap<String,Double>();}
		str = str.replace("{", "");
		str = str.replace("}","");
		
		String[] objs = str.split(",");
		HashMap<String,Double> map = new HashMap<String, Double>();
		try {
			for (int i = 0; i < objs.length; i++) {
				String obj = objs[i];
				int index = obj.lastIndexOf(":");
				String key = obj.substring(0, index);
				String value = obj.substring(index+1);
				map.put(key, Double.parseDouble(value));
			}
		//	System.out.println(map.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return map;
	}
	public String convertToStr(HashMap<String,Double> map) {
		StringBuilder str = new StringBuilder();
		str.append("{");
		for (String key : map.keySet()) {
			Double value = map.get(key);
			str.append(key+":"+value + ",");			
		}
		str.deleteCharAt(str.length()-1);
		str.append("}");
		return str.toString();
	}
	
	StringObjectInspector sinsp;
	MapObjectInspector minsp;
	StringObjectInspector kinsp;
	DoubleObjectInspector vinsp;
	@Override
	public Object evaluate(DeferredObject[] obj) throws HiveException {
		// TODO Auto-generated method stub
		try{
		String nsnf = sinsp.getPrimitiveJavaObject(obj[0].get());
		Map map = minsp.getMap(obj[1].get());
		HashMap<String,Double> nsnfMap = convertToMap(nsnf);
		
		double sum = 0;
		Iterator<Map.Entry> iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = iter.next();
			String key = kinsp.getPrimitiveJavaObject(entry.getKey());
			
			double value=vinsp.get(entry.getValue());
			nsnfMap.put(key,value);
			
	   }
		
	   String str = convertToStr(nsnfMap);

	   return str;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return "";

		
	}

	@Override
	public String getDisplayString(String[] args) {
		// TODO Auto-generated method stub
		//return null;
		return "add a set of key value pairs to json string" + args[0]+ args[1] + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			throw new UDFArgumentException(
					"Add Features takes a string object and a map as arguments");
		}
		ObjectInspector s = args[0];
		Category sc = s.getCategory();
		if (sc == Category.PRIMITIVE) {
			 sinsp = (StringObjectInspector)args[0];
		} else {
			throw new UDFArgumentException(
					" Entropy takes a string object as the first argument");
			
		}
		
		ObjectInspector m = args[1];		
		Category mc = m.getCategory();
		if (mc == Category.MAP) {
			minsp = (MapObjectInspector) m;
			kinsp = (StringObjectInspector) minsp
					.getMapKeyObjectInspector();
			vinsp = (DoubleObjectInspector) minsp
					.getMapValueObjectInspector();
		}
		else {
			throw new UDFArgumentException(
					" Entropy takes a map object as the second argument");
			
		}
		
		//valueInspector = GenerateInspectorHandle();
		
		
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		//return null;
	}

	
	
	public static void main(String[] args) {
		String str = "{\"q::vl::min_idf_webf2\":5.395696405284293e-05,\"q::vl::min_idf_webf1\":0.00024734452688282484,\"vn::rs::titleMatchImportanceAvg\":1.0,\"vn::rs::deltaHit4HHAvg\":0.949121246558128}";
		System.out.println( new Merger().evaluate(str, str));
	}
}
