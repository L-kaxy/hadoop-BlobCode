package com.l_kaxy.hadoop.hadoop_udf;
 
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 
public final class RemoveQuotesUDF extends UDF {
	public Text evaluate(final Text s) {
		Text result = new Test();
		if (s != null) {
			result = new Text(s.toString().replaceAll("\"", ""));
		}
		return result;
	}
}