package org.example;

import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;


public class MyKreoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(VectorWritable.class);
        kryo.register(LongWritable.class);
        kryo.register(RandomAccessSparseVector.class);
        kryo.register(Vector.class);
    }

}
