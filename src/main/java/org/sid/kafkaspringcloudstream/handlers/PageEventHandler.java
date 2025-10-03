package org.sid.kafkaspringcloudstream.handlers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.sid.kafkaspringcloudstream.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input) -> {
            System.out.println("******************");
            System.out.println(input.toString());
            System.out.println("******************");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return () -> new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"U1":"U2",
                new Date(),
                10+new Random().nextInt(10000));
    }

    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> KStreamFunction(){
        return (input) -> {
            return input.filter((k,v) -> v.duration()>100)
                    // on a fait filter pour qu'on prend on consideration que les pageEvent dont la duree de visite est superieur a 100
                    .map((k,v) -> new KeyValue<>(v.name(),v.duration()))
            // on fait le mapping pour transformer le flux pour avoir comme cle le nom de la page et comme valeur la duree de visite
                    .groupByKey(Grouped.with(Serdes.String(),Serdes.Long()))
                    // on fait le groupByKey pour grouper les donnees par cle
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                    .count(Materialized.as("count-store")) // on fait le count pour compter le nombre de pageEvent par cle
                    .toStream()
                    .map((k,v) -> new KeyValue<>(k.key(),v));
        };
    }
}

