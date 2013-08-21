# datawell

A records generator built with Clojure. 

It randomly generates a string with multiple fields(column) once a time. I built it as a practice of studying Clojure, as well as a data source for my studying of bigdata process.

## Usage
    
    #! bash
    JohnsMM:target john$ java -jar datawell-0.1.0-standalone.jar -h

    Usage:
    
     Switches                       Default  Desc 
     --------                       -------  ----                            
     -h, --no-help, --help          false    Show this help.
     -t, --type                     stdout   The type of the service. Avaliable: ["stdout" "http" "socket"] 
     -p, --port                     9876     The port to bind, used in network service(e.g., http, socket) 
     -i, --interval                 200      The time interval between two consecutive records being generated, in     millisecond. Ignored if the type of service is pull-style(e.g., http) instead of push-style(e.g., stdout,     socket). 
     -m, --maxnum                   5        The max number of records to be generated in push-style server(e.g.,     stdout, socket). After that the server will stop pushing     records                                                    
     -n, --colnum                   9        How many columns in a row.   
     -l, --colength                 15       How many characters can a column hold, e.g., the column size.
     -f, --no-fixedlen, --fixedlen  true     Should the size of every column be the same. 
     -s, --seperator                            What character will be the seperator for columns. This character     will not be in characters seeds.                              
     -w, --no-words, --words        true     Use only lowcase alphabet character as seeds to generate random string    . Otherwise all visiable characters will be used.                                                                      

## License

Copyright © 2013 [John Wang](http://wangjinquan.me "John Wang's Blog")

Distributed under the Eclipse Public License, the same as Clojure.
