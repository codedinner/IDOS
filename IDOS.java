/**
 * @author yonghui
 * @create 2021/6/10
 */


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.regex.Pattern;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.ListBuffer;




public class IDOS {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static boolean uploadfile(String localDirectory,String hdfsDirectory,String fileName){

        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(entries);
            String localFullPath = localDirectory+"/"+fileName;
            String hdfsFullPath = hdfsDirectory+"/"+fileName;
            Path localPath = new Path(localFullPath);
            Path hdfspath = new Path(hdfsDirectory);
            Path hdfsfilepath = new Path(hdfsFullPath);
            boolean status1  = new File(localFullPath).isFile();
            boolean status2 = fileSystem.isDirectory(hdfspath);
            boolean status3 = fileSystem.exists(hdfsfilepath);

            System.out.println(status1);
            System.out.println(status2);
            System.out.println(!status3);

            // 本地文件存在,hdfs目录存在,hdfs文件不存在(防止文件覆盖)
            if(status1 && status2 && !status3){
                fileSystem.copyFromLocalFile(false,false,localPath,hdfsfilepath);
                return true;
            }
            return false;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }



    public static void main(String[] args) throws Exception {


        if (args.length < 1) {
            System.err.println("Usage: IDOS <file> <file> <partations>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("ligand-docker-2");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.setLogLevel("warn");
        String receptor = args[1];
        Broadcast<String> receptor_broadcast = ctx.broadcast(receptor);
        JavaRDD receptor_file = ctx.textFile(args[1],1);
        ArrayList<String> receptor_file_list = new ArrayList<>();
        for (Object rdd : receptor_file.collect()){
            receptor_file_list.add(rdd.toString());
        }
        Broadcast<ArrayList<String>> receptor_file_broadcast = ctx.broadcast(receptor_file_list);

        JavaRDD<String> lines = ctx.textFile(args[0],1);
        lines.cache();

        JavaPairRDD<String, Long> tmp = lines.zipWithIndex();
        tmp.cache();

        //inputfprmat
        JavaRDD<String> inputformat_lines = ctx.newAPIHadoopFile(args[0],);

//        System.out.println("tmp.take(3)===>"+tmp.take(3));
//        ctx.newAPIHadoopFile();



        List<Long> lookup = tmp.lookup("$$$$");
        ArrayList<Long> lookup2 = new ArrayList<>();
        lookup2.addAll(lookup);
        JavaRDD keys = tmp.keys();
        keys.cache();
//        System.out.println("-------------------");
//        System.out.println("keys.take(3):"+keys.take(3));
//        System.out.println("lookup:"+lookup);

        ArrayList<Long> index2 = new ArrayList<>();
        Long flagindex = 0l;
        Long lastindex = -1l;
        for (int i=0; i<lookup2.size();i++){
            Long thisindex = lookup2.get(i);
//            System.out.println("thisindex = "+thisindex);
            for (int j=0; j<thisindex-lastindex; j++){
                index2.add(flagindex);
            }
            lastindex = thisindex;
            flagindex++;
        }
//        System.out.println("index2:"+index2);
//        System.out.println("index2.size():"+index2.size());

//        tmp.saveAsTextFile("myoutputligand_tmp");
        List<Tuple2<Long,String>> ligand_list222222 = new ArrayList<>();


        int yhflag = 0;
        for (Object rdd: tmp.collect()){
            String thisstring = rdd.toString();
            int thisstring_length = thisstring.length();
            int part_flag = 0;
            for (int i = thisstring_length-1; i >=0 ; i--) {
                if (thisstring.charAt(i) == ','){
                    part_flag = i;
                    break;
                }
            }
            String thisstring2 = thisstring.substring(1,part_flag);
            Tuple2<Long,String> thispair2 = new Tuple2(index2.get(yhflag),thisstring2);
            yhflag++;
            ligand_list222222.add(thispair2);
        }



//        System.out.println("ligand_list222222.get(0)"+ligand_list222222.get(0));
        HashMap<Long,String> hashMap = new HashMap<>();
        for (Tuple2<Long,String> rddline: ligand_list222222){
            String thisstring = rddline._2;
            Long ligandcode = new Long(rddline._1);
            if (hashMap.containsKey(ligandcode)){
                String tmplist = hashMap.get(ligandcode);
                String newString = tmplist+"￥"+thisstring;

                hashMap.put(ligandcode,newString);
            }else {
                List<String> ligandlist = new ArrayList<>();
                ligandlist.add(thisstring);
                hashMap.put(ligandcode,thisstring);
            }

        }
        List<Tuple2<Long,String>> ligand_list444444 = new ArrayList<>();
        for(Long ligandoce : hashMap.keySet()){
            Tuple2<Long,String> tmptuple = new Tuple2<>(ligandoce,hashMap.get(ligandoce));
            ligand_list444444.add(tmptuple);
        }
        JavaPairRDD<Long,String> mapjavapairRDD = ctx.parallelizePairs(ligand_list444444,6);
//        mapjavapairRDD.saveAsTextFile("mapjavapairRDD");
        JavaRDD ligand_finalrdd = mapjavapairRDD.values();

        System.out.println("wait dock...");


        FlatMapFunction<Iterator<String>,String> function3333 = new  FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> it) throws Exception {
                RuntimeMXBean runtimeMXBean2 = ManagementFactory.getRuntimeMXBean();
                System.out.println(runtimeMXBean2.getName());
                int thispid3 =  Integer.valueOf(runtimeMXBean2.getName().split("@")[0])
                        .intValue();
                System.out.println("+-*/mappartions-function3333------thispid"+thispid3);
                System.out.println("+-*/mappartions-function3333-----integer:=====>"+it);
                ArrayList<String> list1111 = new ArrayList<>();
                list1111.add("------");
                list1111.add("++++++");
                ArrayList<String> computeresu = new ArrayList<>();

                while (it.hasNext()){
                    String ligand = it.next();
                    String[] ligand2= ligand.split("￥");
                    computeresu.addAll(Arrays.asList(ligand2));
                }
                Random rr = new Random();
                int ran1 = rr.nextInt(100);
                String pthstr=String.format("/tmp/%dligand%d.sdf",thispid3,ran1);
                BufferedWriter bw = new BufferedWriter(new FileWriter(pthstr));
                for (int i = 0; i < computeresu.size(); i++) {
                    bw.write(computeresu.get(i));
                    bw.newLine();
                    bw.flush();
                }

                try {
                    Process process = Runtime.getRuntime().exec("echo hello-fun33333");
                    int waitForcode = process.waitFor();
                    System.out.println("waitForcode>>>>"+waitForcode);
                    BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                    final OutputStream is2 = process.getOutputStream();
                    System.out.println("hello-fun33333-->is2==="+is2.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    String receptor = receptor_broadcast.getValue();
                    ArrayList<String> receptor_file_b = receptor_file_broadcast.getValue();
                    String receptor_pthstr=String.format("/tmp/receptor_%d.pdb",thispid3);
                    BufferedWriter bw_2 = new BufferedWriter(new FileWriter(receptor_pthstr));
                    for (int i = 0; i < receptor_file_b.size(); i++) {
//                    System.out.println("computeresu.get(i)"+computeresu.get(i));
                        bw_2.write(receptor_file_b.get(i));
                        bw_2.newLine();
                        bw_2.flush();
                    }
                    System.out.println("hello-dock-fun33333>>>=="+receptor);
//                    String dock_cmd=String.format("nohup /root/D3DOCK/D3DOCKxb_v2/build/linux/release/D3DOCKxb --seed 0 --autobox_ligand %s -r %s -l %s -o /tmp/OUTslave1cmpd_D3DOCKxb.sdf > /tmp/testlog.log 2>&1 &",receptor_pthstr,receptor_pthstr,pthstr,thispid3);
//                    String dockresu = "/tmp/AAAAA";
                    String dockresu_file=String.format("dockresu_%d.pdb",thispid3);
                    String dockresu = "/tmp"+"/"+dockresu_file;
                    String dock_log = "/root"+"/"+dockresu_file+"_.log";




                    String dock_cmd=String.format("sh /root/dock_shell.sh %s %s %s",receptor_pthstr,pthstr,dockresu);

                    System.out.println("dock_cmd====="+dock_cmd);
                    Runtime runtimedock = Runtime.getRuntime();
                    Process pdock = runtimedock.exec(dock_cmd);
                    new Thread(new Runnable() {

                        @Override
                        public void run() {
                            BufferedReader br = new BufferedReader(
                                    new InputStreamReader(pdock.getErrorStream()));
                            try {
                                while (br.readLine() != null)
                                    System.out.println(br.readLine());
                                br.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();


                    BufferedInputStream in = new BufferedInputStream(pdock.getInputStream());
                    BufferedReader inBr = new BufferedReader(new InputStreamReader(in));
                    String lineStr;
                    while ((lineStr = inBr.readLine())!=null){
                        System.out.println(lineStr);
                    }

//                    BufferedInputStream inerrot = new BufferedInputStream(pdock.getErrorStream());
//                    BufferedReader inBrerrot = new BufferedReader(new InputStreamReader(inerrot));
//                    String lineStrerror;
//                    while ((lineStrerror = inBrerrot.readLine())!=null){
//                        System.out.println(lineStrerror);
//                    }

                    System.out.println("waitForcode2222>>>>"+pdock.waitFor());
                    if (pdock.waitFor()!=0){
                        System.out.println("dock faied");
                    }
//                    pdock.destroy();
                    //上传文件
                    boolean uploadStatus = uploadfile("/tmp", "/user/root/output", dockresu_file);
                    if (uploadStatus){
                        System.out.println("upload success!");
                    }else {
                        System.out.println("upload faled!");
                    }
                    BufferedWriter bout_dock = new BufferedWriter(new OutputStreamWriter(pdock.getOutputStream()));

                } catch (IOException e) {
                    e.printStackTrace();
                }



                return list1111.iterator();
            }
        };


        JavaRDD ligand_finalrdd2 = ligand_finalrdd.mapPartitions(function3333);
        ligand_finalrdd2.collect();

        ctx.stop();
    }

}

