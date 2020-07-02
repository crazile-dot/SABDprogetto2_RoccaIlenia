public class Query1MainClass {

    public static void main(String[] args) throws Exception {

        /*Tuple4<StreamExecutionEnvironment, StreamingFileSink<Double>, StreamingFileSink<Tuple2<String, Integer>>, FlinkKafkaConsumer<String>> tuple3 = Environment.getEnvironment();
        StreamExecutionEnvironment environment = tuple3.f0;
        StreamingFileSink<Double> sink = tuple3.f1;
        FlinkKafkaConsumer<String> consumer = tuple3.f3;*/

        //DataStream<String> stream = environment.addSource(consumer);
        /*DataStream<Failure> parsed = environment.addSource(consumer).map(s -> CsvParser.customized1Parsing(s))
              .filter(f -> f != null && !f.getOccurredOn().equals("") && f.getHowLongDelayed() != 0);
        DataStream<Tuple3<DateTime, String, Integer>> stream = parsed
                .map(new MapFunction<Failure, Tuple3<DateTime, String, Integer>>() {
                    @Override
                    public Tuple3<DateTime, String, Integer> map(Failure failure) throws Exception {
                        return new Tuple3<>(failure.getOccurredOn(), failure.getBoro(), failure.getHowLongDelayed());
                    }
                });
        SingleOutputStreamOperator<Double> out = stream.keyBy(1).timeWindow(Time.seconds(5))
                .aggregate(new AverageAggregate());
        out.print();*/
                /*.reduce(new ReduceFunction<Tuple1<Failure>>() {
                    @Override
                    public Tuple1<Failure> reduce(Tuple1<Failure> failure1, Tuple1<Failure> failure2) throws Exception {
                        Failure newFail = new Failure(failure1.f0.getOccurredOn(), failure1.f0.getBoro(), 0, new AverageAggregate());
                        return new Tuple1<>(newFail);
                    }
                });*/
        /*stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out ) {
                String[] tokens = s.toLowerCase().split( "," );

                // emit the pairs
                for(String token : tokens){
                    if(token.length() > 0){
                        out.collect( new Tuple2<String, Integer>( token, 1 ) );
                    }
                }
            }
    }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).addSink(sink);*/
        //environment.execute("Query1Program");
    }
}
