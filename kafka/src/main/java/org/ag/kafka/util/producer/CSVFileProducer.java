package org.ag.kafka.util.producer;

import org.ag.kafka.config.ProducerConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.FileReader;
import java.io.IOException;



/**
 * Created by ahmed.gater on 28/11/2016.
 */
public class CSVFileProducer {
    CSVParser parser ;
    ProducerConfig prodConfig;
    SimpleProducer<String, String> producer;

    public CSVFileProducer(CSVParser parser,ProducerConfig prodCfg){
        this.parser = parser ;
        this.prodConfig = prodCfg;
        this.producer = new SimpleProducer<String, String>(prodConfig);
    }

    public CSVFileProducer(String filePath, char fieldDelimiter){
        try {
            parser = new CSVParser( new FileReader(filePath),
                    CSVFormat.DEFAULT.withDelimiter(fieldDelimiter).withHeader());

        } catch (IOException e  ){
            e.printStackTrace();
        }
    }

    public void sendAll() throws IOException {
        this.parser.forEach(x ->
        {
            this.producer.send(x.toMap().toString(), x.toMap().toString());
            System.out.println(x);
        }) ;
    }


    public static void main(String[] args) throws IOException {
        String filePath = "C:\\Users\\ahmed.gater\\.VirtualBox\\Desktop\\Raw data after import\\Raw data after import.csv" ;
        CSVParser parser;
        parser = new CSVParser( new FileReader(filePath),
                CSVFormat.DEFAULT.withDelimiter(';').withHeader());
        ProducerConfig cfg = new ProducerConfig.ProducerConfigBuilder("192.168.99.100:9092", "spark-streaming-test")
                .build();

        CSVFileProducer prod = new CSVFileProducer(parser,cfg) ;
        prod.sendAll();
    }
}
