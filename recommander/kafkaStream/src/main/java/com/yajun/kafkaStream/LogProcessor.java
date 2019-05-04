package com.yajun.kafkaStream;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[],byte[]> {

    public static final String PREFIX_MSG = "abc:";

    // context
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;

    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        String ratingValue = new String(bytes2);

        if(ratingValue.contains(PREFIX_MSG)){
            String bValue = ratingValue.split(PREFIX_MSG)[1];
            context.forward("log".getBytes(),bValue.getBytes());
        }

    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
