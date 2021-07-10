# Dependency Injection of SparkSession: Apache Spark on/with support of Spring boot
![spark-spring.jpg](spark-spring.jpg)
* A Web Application of Apache Spark using Spring Boot MVC
* Spring is a very popular Java-based framework for building web and enterprise applications.
* Spring framework provides flexibility to configure beans/objects in multiple ways such as XML, Annotations, and JavaConfig.
* Spring’s dependency injection approach encourages writing testable code
* Spring Boot is basically an extension of the Spring framework which eliminated the boilerplate configurations required for setting up a Spring application.
* Apache Spark is an open-source distributed general-purpose cluster-computing framework.

## Mixing Spring & Spark - Steps
1. Create a Maven-Based Spring Boot Project
2. Add the required dependencies in pom.xml
3. Configure Spark Properties in application.properties
4. Adding the Spark ApplicationConfig: declaring the JavaSparkContext and SparkConf as beans (using @Bean annotation) this tell the spring container to manage them for us
5. Creating a service/controller for Read Csv & Register a REST Controller with an endpoint
6. Run the application, Test your application from a REST client or on Browser
   Url: http://localhost:9090/spark-context/task name

### That's it, Spark now returns output on an Enterprise web application!
We are going to make a processing on the Wazzaf Data Set and applying the 
following tasks 
1 - Displying the Print the Schema Discribtion by using this 
link http://localhost:9090/spark-wuzzuf-context/show_data

2- Displying the Wazzaf Stracture by using this link
http://localhost:9090/spark-wuzzuf-context/show_structure

3- Displying the Wazzaf Summary by using this link
http://localhost:9090/spark-wuzzuf-context/show_summary

4- Displying the Wazzaf Data Set without Null Values by using this link
http://localhost:9090/spark-wuzzuf-context/clean_data

5- Displying the Wazzaf Counting Jobs Values by using this link
http://localhost:9090/spark-wuzzuf-context/show_count_jobs

6- Displying the Wazzaf by Drawing Charts Jobs by using this link
http://localhost:9090/spark-wuzzuf-context/chart_jobs

7- Displying the Wazzaf by Drawing Charts Jobs Title  by using this link
http://localhost:9090/spark-wuzzuf-context/show_job_title

8- Displying the Wazzaf by Drawing Charts Jobs Titles  by using this link
http://localhost:9090/spark-wuzzuf-context/chart_title

9- Displying the Wazzaf by Drawing Charts Areas  by using this link
http://localhost:9090/spark-wuzzuf-context/chart_areas

10 - Displying the Wazzaf by Drawing Charts Areas  by using this link
http://localhost:9090/spark-wuzzuf-context/skills

11- Displying the Wazzaf by Drawing Charts Years of Experience by using this link
http://localhost:9090/spark-wuzzuf-context/show_exp