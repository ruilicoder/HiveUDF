package hive.udf;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
@Description(name = "entropy", value = "_FUNC_(map) - Returns the entropy.")
//Compute Entropy of a Function
public class Entropy  extends GenericUDF{
	MapObjectInspector minsp;
	StringObjectInspector kinsp;
	StringObjectInspector vinsp;
	@Override
	public Object evaluate(DeferredObject[] obj) throws HiveException {
		// TODO Auto-generated method stub
		//StringWriter writer = new StringWriter();
		Map map = minsp.getMap(obj);
		double sum = 0;
		Iterator<Map.Entry> iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = iter.next();
			String key = kinsp
					.getPrimitiveJavaObject(entry.getKey());
			
			String value=vinsp.getPrimitiveJavaObject(entry.getValue());
			String[] values = value.split("\t");
			String freq = values[0];
			String preward = values[1];
			sum = sum + Double.parseDouble(freq);
		}
		
		return sum;
	}

	@Override
	public String getDisplayString(String[] args) {
		// TODO Auto-generated method stub
		//return null;
		return "entropy(" + args[0] + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			throw new UDFArgumentException(
					" Entropy takes a map object as an argument");
		}
		ObjectInspector oi = args[0];
		Category cat = oi.getCategory();
		if (cat == Category.MAP) {
			minsp = (MapObjectInspector) oi;
			kinsp = (StringObjectInspector) minsp
					.getMapKeyObjectInspector();
			vinsp = (StringObjectInspector) minsp
					.getMapValueObjectInspector();
		}
		else {
			throw new UDFArgumentException(
					" Entropy takes a map object as an argument");
			
		}
		
		//valueInspector = GenerateInspectorHandle();
		
		
		return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

		//return null;
	}

	
	
}
