package com.yunpengn.assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommend {
  public static final String HDFS = "*********";

  public static final Pattern DELIMITER = Pattern.compile("[\t,]");

  public static void main(String[] args) throws Exception {
    Map<String, String> path = new HashMap<>();

    //path for local data
    path.put("data", "******datapath*****");

    //step1 i/o path
    path.put("Step1Input", HDFS + "/recommend");
    path.put("Step1Output", path.get("Step1Input") + "/step1");

    //step2 i/o path
    path.put("Step2Input", path.get("Step1Output"));
    path.put("Step2Output", path.get("Step1Input") + "/step2");

    //step3_1 i/o path
    path.put("Step3Input1", path.get("Step1Output"));
    path.put("Step3Output1", path.get("Step1Input") + "/step3_1");

    //step3_2 i/o path
    path.put("Step3Input2", path.get("Step2Output"));
    path.put("Step3Output2", path.get("Step1Input") + "/step3_2");

    //step4 i/o path
    path.put("Step4_1Input1", path.get("Step3Output1"));
    path.put("Step4_1Input2", path.get("Step3Output2"));
    path.put("Step4_1Output", path.get("Step1Input") + "/step4_1");
    path.put("Step4_2Input", path.get("Step4_1Output"));
    path.put("Step4_2Output", path.get("Step1Input") + "/step4_2");

    //step5 i/o path
    path.put("Step5Input1", path.get("Step4_2Output"));
    path.put("Step5Input2", path.get("Step1Input") + "****data****");
    path.put("Step5Output", path.get("Step1Input") + "/step5");


    Step1.run(path);
    Step2.run(path);
    Step3.run1(path);
    Step3.run2(path);
    Step4_1.run(path);
    Step4_2.run(path);
    Step5.run(path);

    // Example to show result
    HDFSAPI hdfs = new HDFSAPI(new Path(HDFS));
    System.out.println(path.get("Step5Output") + "/part-r-00000");
    hdfs.readFile(new Path(path.get("Step5Output") + "/part-r-00000"));
    System.exit(0);
  }

  public static Configuration config() {
    return new Configuration();
  }
}
