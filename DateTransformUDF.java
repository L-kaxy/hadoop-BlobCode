package com.l_kaxy.hadoop.hadoop_udf;
 
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 
public final class DateTransformUDF extends UDF { 

	private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	public Text evaluate(final Text s) {
		Text result = null;
		if (s != null) {

			String inputDate = s.toString().trim();

			try {

				Date date = inputFormat.parse(inputDate);
				result = new Text(outputFormat.format(date));

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
}