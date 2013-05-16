package com.ipjmc.cluster;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * K-Means聚类算法演示，将平面上的9个点，分为2类
 * @author wylazy
 *
 */
public class KMeansCluster {

	public static final double[][] points = { { 1, 1 }, { 2, 1 }, { 1, 2 },
			{ 2, 2 }, { 3, 3 }, { 8, 8 }, { 9, 8 }, { 8, 9 }, { 9, 9 } };

	/**
	 * Mahout要求K-Means的输入为SequenceFile，所以要将这9个点写入SeqenceFile
	 * @param points
	 * @param fileName
	 * @param fs
	 * @param conf
	 * @throws IOException
	 */
	public static void writePointsToFile(List<Vector> points, String fileName,
			FileSystem fs, Configuration conf) throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}

	/**
	 * 将平面上的点转化为vector
	 * @param raw
	 * @return
	 */
	public static List<Vector> getPoints(double[][] raw) {
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}
		return points;
	}

	public static void main(String[] args) throws Exception {

		int k = 2;
		
		//先将点转化为vector，然后再将vector写入SequeceFile
		List<Vector> vectors = getPoints(points);

		File testData = new File("testdata");
		if (!testData.exists()) {
			testData.mkdir();
		}
		
		testData = new File("testdata/points");
		if (!testData.exists()) {
			testData.mkdir();
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		writePointsToFile(vectors, "testdata/points/file1", fs, conf);

		
		//这里聚类时只有两类，所以写入K-Means初始化时的两个点
		Path path = new Path("testdata/clusters/part-00000");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				Text.class, Kluster.class);

		for (int i = 0; i < k; i++) {
			Vector vec = vectors.get(i);

			Kluster kluster = new Kluster(vec, i,
					new EuclideanDistanceMeasure());
			writer.append(new Text(kluster.getIdentifier()), kluster);
		}
		writer.close();

		//调用K-Means算法开始聚类
		KMeansDriver.run(conf, new Path("testdata/points"), new Path(
				"testdata/clusters"), new Path("output"),
				new EuclideanDistanceMeasure(), 0.001, 10, true, 0, false);

		//打印聚类的结果
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
				"output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"),
				conf);

		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			System.out.println(value.toString() + " belongs to cluster "
					+ key.toString());
		}
		reader.close();
	}
}
