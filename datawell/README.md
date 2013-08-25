# datawell

A records generator built in Clojure with Leiningen. 

It randomly generates a string(record) with multiple fields(column) once a time. It can server records in push-style in either Socket, by which it acts as socket server pushing records to those connected socket clients, or simply print(push) them out on STDOUT which can be easily redirected to a file. It can also be used as pull-style data generating service through HTTP. 

I built it as a practice of studying Clojure, as well as a data source for my studying of bigdata process.

## Usage
    
    JohnsMM:target john$ java -jar datawell-0.1.0-standalone.jar -h

    Usage:
    
     Switches                       Default  Desc 
     --------                       -------  ----                            
     -h, --no-help, --help          false    Show this help.
     -t, --type                     stdout   The type of the service. Avaliable: ["stdout" "http" "socket"] 
     -p, --port                     9876     The port to bind, used in network service(e.g., http, socket) 
     -i, --interval                 200      The time interval between two consecutive records being generated, in millisecond. Ignored if the type of service is pull-style(e.g., http) instead of push-style(e.g., stdout, socket). 
     -m, --maxnum                   5        The max number of records to be generated in push-style server(e.g., stdout, socket). After that the server will stop pushing records                                                    
     -n, --colnum                   9        How many columns in a row.   
     -l, --colength                 15       How many characters can a column hold, e.g., the column size.
     -f, --no-fixedlen, --fixedlen  true     Should the size of every column be the same. 
     -s, --seperator                (tab)    What character will be the seperator for columns. This character will not be in characters seeds.                              
     -w, --no-words, --words        true     Use only lowcase alphabet character as seeds to generate random string. Otherwise all visiable characters will be used.

To generate 5 random records with 5 columns each:

    JohnsMM:target john$ java -jar datawell-0.1.0-standalone.jar --no-fixedlen -n 5 -l 5
    rtfp   gm     lvmgq    drus    lwdjp
    zwf    rqj    mhdwe    ezhc    lpa
    numr   hhle   eic      zci     d
    sszv   fnad   i        lta     aqr
    d      wduh   imyz     bdni    lf

    JohnsMM:target john$ 

To start a http service on port 9000 which response a random record on every request:

    JohnsMM:target john$ java -jar datawell-0.1.0-standalone.jar -t http -p 9000
    2013-08-25 13:30:44.765:INFO:oejs.Server:jetty-7.x.y-SNAPSHOT
    2013-08-25 13:30:44.904:INFO:oejs.AbstractConnector:Started SelectChannelConnector@0.0.0.0:9000

    JohnsMM:target john$ 


## License

Copyright © 2013 [John Wang](http://wangjinquan.me "John Wang's Blog")

Distributed under the Eclipse Public License, the same as Clojure.
