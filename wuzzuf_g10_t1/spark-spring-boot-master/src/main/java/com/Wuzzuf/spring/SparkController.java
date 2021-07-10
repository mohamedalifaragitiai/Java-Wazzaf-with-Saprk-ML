package com.Wuzzuf.spring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.io.IOException;
import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.http.MediaType;
import scala.Tuple2;

@RequestMapping("spark-wuzzuf-context")
@Controller
public class SparkController {

    @Autowired
    private  SparkSession sparkSession ;

    public SparkController(SparkSession sparkSession) {
        this.sparkSession = sparkSession.builder ().appName ("WUZZUF Analysis Demo").master ("local[6]")
                .getOrCreate ();
    }

    public Dataset<Row> read_data (){

        return sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_jobs.csv");
    }




    @RequestMapping("show_data")
    public ResponseEntity<String> Wuzzuf_show_data() {
        Dataset<Row> wuzzufDF = read_data();
        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
        String.format("<h2>%s</h2>", "Spark version = "+sparkSession.sparkContext().version()) +
        String.format("<h3>%s</h3>", "Read csv..") +
        String.format("<h4>Total records %d</h4>", wuzzufDF.count()) +
        //String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", wuzzufDF.schema().treeString()) +
        //String.format("<h4>Describe %d</h4>", wuzzufDF.describe()) +
        wuzzufDF.showString(10, 10, true);
        return ResponseEntity.ok(html);

    }
    @RequestMapping("show_structure")
    public ResponseEntity<String> Wuzzuf_show_structure() {
        Dataset<Row> wuzzufDF = read_data();
        String html = String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", wuzzufDF.schema().treeString()) +
                      String.format("<h4>Describe %d</h4>") +
                      wuzzufDF.describe().showString(5,5,true);
        return ResponseEntity.ok(html);

    }
    @RequestMapping("show_summary")
    public ResponseEntity<String> Wuzzuf_show_summary() {
        Dataset<Row> wuzzufDF = read_data().summary();
        String html = String.format("<h5>Summary <br/> %s</h5> <br/> Sample data - <br/>")+
                      wuzzufDF.summary().showString(5,5,true);
        return ResponseEntity.ok(html);
    }

    @RequestMapping("clean_data")
    public  ResponseEntity<String> Wuzzuf_remove_null() {
        Dataset<Row> wuzzufDF = read_data();
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        String html = String.format("<h4>dataset count after remove null/duplicates %d</h4>", wuzzufNoNullDF.count());
        return ResponseEntity.ok(html);
    }

    @RequestMapping("show_count_jobs")
    public  ResponseEntity<String> Wuzzuf_count_jobs() {
        Dataset<Row> wuzzufDF = read_data();
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        Dataset<Row> wuzzuf_comp = wuzzufNoNullDF.groupBy("Company").count().sort(functions.desc("count"));

        String html = String.format("<h4>Show the most demanding companies %d</h4>") +
                String.format("<h4>jobs count %d</h4>",wuzzuf_comp.count())+
                wuzzuf_comp.showString(5,5,true);
        return ResponseEntity.ok(html);
    }


    @GetMapping(value = "chart_jobs", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> image() throws IOException {
        Dataset<Row> wuzzufDF = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_jobs.csv");
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        Dataset<Row> wuzzuf_comp = wuzzufNoNullDF.groupBy("Company").count().sort(functions.desc("count"));
        List<String> CompanyName = wuzzuf_comp.select("Company").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> Company_Num = wuzzuf_comp.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        PieChart_plot(CompanyName,Company_Num,"jobs for each company");

        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "./src/main/resources/Counter JobsperCompany.jpg"
        )));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);

    }

    @RequestMapping("show_job_title")
    public  ResponseEntity<String> Wuzzuf_job_title() {
        Dataset<Row> wuzzufDF = read_data();
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        Dataset<Row> wuzzuf_title = wuzzufNoNullDF.groupBy("Title").count().sort(functions.desc("count"));

        String html = String.format("<h4>Show the most popular job title %d</h4>") +
                      String.format("<h4>titles count %d</h4>",wuzzuf_title.count())+
                      wuzzuf_title.showString(5,5,true);
        return ResponseEntity.ok(html);
    }



    @GetMapping(value = "chart_title", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> image2() throws IOException {
        Dataset<Row> wuzzufDF = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_jobs.csv");
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        Dataset<Row> wuzzuf_title = wuzzufNoNullDF.groupBy("Title").count().sort(functions.desc("count"));
        List<String> TitleName = wuzzuf_title.select("Title").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> Title_Num = wuzzuf_title.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        BarChart_plot(TitleName,Title_Num,"most popular job titles","Title_count");

        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "./src/main/resources/most popular job titles.jpg"
        )));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);

    }

    @RequestMapping("show_areas")
    public  ResponseEntity<String> Wuzzuf_areas() {
        Dataset<Row> wuzzufDF = read_data();
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        Dataset<Row> wuzzuf_areas = wuzzufNoNullDF.groupBy("Location").count().sort(functions.desc("count"));

        String html = String.format("<h4>Show the  most popular areas %d</h4>") +
                String.format("<h4>areas count %d</h4>",wuzzuf_areas.count())+
                wuzzuf_areas.showString(5,5,true);
        return ResponseEntity.ok(html);
    }

    @GetMapping(value = "chart_areas", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> image3() throws IOException {
        Dataset<Row> wuzzufDF = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_jobs.csv");
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        Dataset<Row> wuzzuf_areas = wuzzufNoNullDF.groupBy("Location").count().sort(functions.desc("count"));
        wuzzuf_areas.show();
        List<String> LocationName = wuzzuf_areas.select("Location").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> Location_Num = wuzzuf_areas.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        BarChart_plot(LocationName,Location_Num,"most popular areas","Areas_count");

        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "./src/main/resources/most popular areas.jpg"
        )));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);

    }

    @GetMapping("skills")
    @ResponseStatus(code = HttpStatus.OK)
    public ResponseEntity<String> wuzzuf_skills() {
        Dataset<Row> wuzzufDF = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_jobs.csv");
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();

        Dataset<Row> skills = wuzzufNoNullDF.select("Skills").flatMap(row -> Arrays.asList(row.getString(0).split(" "))
                .iterator(), Encoders.STRING())
                .filter(s -> !s.isEmpty())
                .map(word -> new Tuple2<>(word.toLowerCase(), 1L), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
                .toDF("Skills", "count")
                .groupBy("Skills")
                .sum("count").orderBy(new Column("sum(count)").desc()).withColumnRenamed("sum(count)", "_cnt");

        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h4>skills count %d</h4>",skills.count())+
                        skills.showString(10, 10, true) ;
        return ResponseEntity.ok(html);

    }


    @GetMapping("show_exp")
    @ResponseStatus(code = HttpStatus.OK)
    public ResponseEntity<String> wuzzuf_yearsOfExp() {
        Dataset<Row> wuzzufDF = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_jobs.csv");
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();

        Dataset<Row> wuzzuf_Exp = wuzzufNoNullDF.withColumn("YearsExp", functions.split(functions.col("YearsExp"), "-|\\+| ").getItem(0))
                .withColumn("YearsExp", functions.when(functions.col("YearsExp").equalTo("null"), "0").otherwise(functions.col("YearsExp")))
                .withColumn("YearsExp", functions.col("YearsExp").cast("integer"));


        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h4>YrsOfExp count %d</h4>",wuzzuf_Exp.count())+
                wuzzuf_Exp.showString(10, 10, true) ;
        return ResponseEntity.ok(html);

    }





    public void BarChart_plot(List<String> company_list, List<Long> company_count, String Chart_name, String barName) throws IOException {
        CategoryChart chart = new CategoryChartBuilder().width (1024).height (768).title (Chart_name).build ();
        // Customize Chart
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        // Series
        chart.addSeries (barName, company_list, company_count);
        // Show it
        BitmapEncoder.saveBitmap(chart, "./src/main/resources/" + Chart_name, BitmapEncoder.BitmapFormat.JPG);


    }

    public void PieChart_plot(List<String> company_list,List<Long> company_count,String Chart_name) throws IOException {

        PieChart chart = new PieChartBuilder().width (800).height (600).title (getClass ().getSimpleName ()).title(Chart_name).build ();
        // Customize Chart
        //Color[] sliceColors = new Color[]{new Color (180, 68, 50), new Color (130, 105, 120), new Color (80, 143, 160)};
        //chart.getStyler ().setSeriesColors (sliceColors);

        for(int i = 0;i < company_list.size();i++){
            chart.addSeries(company_list.get(i), company_count.get(i));
        }

        // Show it
        BitmapEncoder.saveBitmap(chart, "./src/main/resources/" + "Counter JobsperCompany", BitmapEncoder.BitmapFormat.JPG);
    }

    static int countStringOccurance(ArrayList<String> arr, String str){
        int count = 0;
        for (int i=0; i<arr.size(); i++) {
            if (str.equals(arr.get(i))) {
                count += 1;
            }
        }
        return count;
    }


    public static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }





}
