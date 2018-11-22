package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 	��������
     *        ˵���˾���ʹ��SparkSQL���������ǵķ���ȡTopN
 *
 */

public class RowNumberWindowFunction {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sc.sc());
		
		//�������۶sales��
		hiveContext.sql("DROP TABLE IF EXISTS sales");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("+
		"produce STRING,"+"category STRING,"+"revenue BIGINT)");
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/sales.txt' INTO TABLE sales");
		
		//��ʵ�����Ǹ�ÿһ����������ݣ�����������˳�򣬴���һ��������кţ�����˳��ţ�
		//����˵����һ������date=20160706 ��������3������,11211  11212  11213
		//��ô����������ÿһ������ʹ��row_number()���������Ժ������л����һ�����ڵ��к�
		//�кŴ�1��ʼ����  ���Ľ������ 11211 1    ,   11212 2   ,  11213  3
		
		Dataset<Row> top3SaleDF = hiveContext.sql("SELECT product , category , revenue"
				+"FROM("
				+"SELECT product , category , revenue, "
				+ "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
				+ "FROM sales) tmp_sales "
				+ "WHERE rank <= 3");
		//��ÿ������ǰ�������ݣ����浽һ������
		
		hiveContext.sql("DROP TABLE IF EXISTS top3_sales");
		
		sc.close();
		
	}
}
