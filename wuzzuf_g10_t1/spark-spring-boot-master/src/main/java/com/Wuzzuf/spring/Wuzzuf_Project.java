package com.Wuzzuf.spring;

import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.util.*;
import java.util.stream.Collectors;

public class Wuzzuf_Project {
    public static void main(String[] args) {


        // Create Spark Session to create connection to Spark
        Wuzzuf_Project xChart_wuzzuf = new Wuzzuf_Project ();
        final SparkSession sparkSession = SparkSession.builder ().appName ("WUZZUF Analysis Demo").master ("local[6]")
                .getOrCreate ();
        sparkSession.sparkContext().setLogLevel("ERROR");


        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();



        // Set header option to true to specify that first row in file contains ///////
        dataFrameReader.option ("header", "true");
        Dataset<Row> wuzzufDF = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");

        // Print Schema to see column names, types and other metadata
        wuzzufDF.show(10);
        wuzzufDF.printSchema ();
        wuzzufDF.describe().show();
        System.out.println ("DataSet Size before null&duplicate remove is " +wuzzufDF.count ());


        //============================================================================================================
        // remove null and duplicate from data
        Dataset<Row> wuzzufNoNullDF =wuzzufDF.dropDuplicates().na().drop ();
        wuzzufNoNullDF.printSchema ();
        System.out.println ();
        System.out.println ("Dataset Size after null&duplicate remove is "+wuzzufNoNullDF.count ());

        //============================================================================================================
        // plot pie chart for company
        Dataset<Row> wuzzuf_comp = wuzzufNoNullDF.groupBy("Company").count().sort(functions.desc("count"));
        wuzzuf_comp.show();
        List<String> CompanyName = wuzzuf_comp.select("Company").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> Company_Num = wuzzuf_comp.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        xChart_wuzzuf.PieChart_plot(CompanyName,Company_Num,"jobs for each company");

        //========================================================================================================
        //
        Dataset<Row> wuzzuf_title = wuzzufNoNullDF.groupBy("Title").count().sort(functions.desc("count"));
        wuzzuf_title.show();
        List<String> TitleName = wuzzuf_title.select("Title").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> Title_Num = wuzzuf_title.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        xChart_wuzzuf.BarChart_plot(TitleName,Title_Num,"most popular job titles","Title_count");




        //========================================================================================================

        Dataset<Row> wuzzuf_areas = wuzzufNoNullDF.groupBy("Location").count().sort(functions.desc("count"));
        wuzzuf_areas.show();
        List<String> LocationName = wuzzuf_areas.select("Location").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> Location_Num = wuzzuf_areas.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        xChart_wuzzuf.BarChart_plot(LocationName,Location_Num,"most popular areas","Areas_count");

        //========================================================================================================

        List<String> Skills_list = wuzzufNoNullDF.select("Skills").as(Encoders.STRING()).collectAsList();

        ArrayList<String> skills_list_sep = new ArrayList<String>();
        ArrayList<String> skills_list_sep_d = new ArrayList<String>();

        for(int k = 0;k<Skills_list.size();k++){

            skills_list_sep.addAll(Arrays.asList(Skills_list.get(k).split("\\s*,\\s*")));
        }

        skills_list_sep_d = (ArrayList<String>) skills_list_sep.stream().distinct().collect(Collectors.toList());
        HashMap<String,Integer> Skill_map=new HashMap<String,Integer>();
        for(int r=0;r < skills_list_sep_d.size() ;r++){

            Skill_map.put(skills_list_sep.get(r),countStringOccurance(skills_list_sep, skills_list_sep_d.get(r)));
        }
        HashMap<String,Integer> Skill_map_or = sortByValue(Skill_map);
        Skill_map_or.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

    }

    public void BarChart_plot(List<String> company_list,List<Long> company_count,String Chart_name,String barName){
        CategoryChart chart = new CategoryChartBuilder().width (1024).height (768).title (Chart_name).build ();
        // Customize Chart
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        // Series
        chart.addSeries (barName, company_list, company_count);
        // Show it
        new SwingWrapper(chart).displayChart ();


    }

    public void PieChart_plot(List<String> company_list,List<Long> company_count,String Chart_name){

        PieChart chart = new PieChartBuilder().width (800).height (600).title (getClass ().getSimpleName ()).title(Chart_name).build ();
        // Customize Chart
        //Color[] sliceColors = new Color[]{new Color (180, 68, 50), new Color (130, 105, 120), new Color (80, 143, 160)};
        //chart.getStyler ().setSeriesColors (sliceColors);

        for(int i = 0;i < company_list.size();i++){
            chart.addSeries(company_list.get(i), company_count.get(i));
        }

        // Show it
        new SwingWrapper (chart).displayChart ();



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



