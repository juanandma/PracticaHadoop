/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package practicahadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author juan
 */
public class Radiacion {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        private Text SESTACION = new Text();
        private FloatWritable radiacion = new FloatWritable();

        public void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context
        ) throws IOException, InterruptedException {

            String[] valores = value.toString().split(";");

            //cambiamos ',' por '.' para tranformar el valor a float
            String nuevoradiacion = valores[DatosProvincia.RADIACION].replaceAll(",", ".");
            //quitamos las comillas para parsear a int
            String nuevoestacion = valores[DatosProvincia.IDESTACION].replaceAll("\"", "");

            int IDESTACION = 0;

            if (!nuevoestacion.equalsIgnoreCase("IDESTACION") && !nuevoestacion.isEmpty()) {
                IDESTACION = Integer.parseInt(nuevoestacion);

            }

            if (!nuevoradiacion.equalsIgnoreCase("RADIACION") && !nuevoradiacion.isEmpty() && valores.length == 18
                    && IDESTACION >= 2 && IDESTACION <= 10) {

                SESTACION.set(valores[DatosProvincia.SESTACION]);

                radiacion.set(Float.parseFloat(nuevoradiacion));

                context.write(SESTACION, radiacion);
            } else {

                //contar como linea no valida
                context.getCounter(COUNTER.INVALID_RECORD_COUNT).increment(1L);
            }

        }
    }

    public static class RadiacionMediaReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                Reducer<Text, FloatWritable, Text, FloatWritable>.Context context
        ) throws IOException, InterruptedException {

            Float media = 0f;
            Float suma = 0f;
            Float cont = 0f;

            for (FloatWritable valor : values) {
                suma += valor.get();
                cont++;
            }

            media = suma / cont;

            this.result.set(media);

            context.write(key, this.result);
        }
    }

    private enum COUNTER {
        INVALID_RECORD_COUNT
    }

    public static void main(String[] args) throws Exception {

        Log log = LogFactory.getLog(TokenizerMapper.class);

        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("argumetos de radiacion: <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "radiacion");
        job.setJarByClass(Radiacion.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(RadiacionMediaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        Counters contador = job.getCounters();

        if (log.isDebugEnabled()) {
            log.debug("Lineas no validas: " + contador.findCounter(COUNTER.INVALID_RECORD_COUNT).getValue());
        }
        System.out.println("Lineas no validas: " + contador.findCounter(COUNTER.INVALID_RECORD_COUNT).getValue());

    }
}
