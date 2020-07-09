# OpenCEP
A generic CEP library in python (requires python version 3.7+).

Complex event processing, or CEP, is event processing that combines data from multiple sources to infer events or patterns that suggest more complicated circumstances.

CEP executes a set of algorithms which, as said, can infer events, patterns and sequences. The input of these algorithms are event streams and patterns, and the result of these calculations are pattern matches, or matches.

The algorithms are accessible for use with the API of this library.

This short documentation will be updated regularly.

# Features
* [X] A mechanism for CEP pattern evaluation based on the acyclic graph model
* [X] "Flat" sequence and conjunction pattern support
* [X] Instance-based memory model
* [X] The pattern is provided as a Python class
* [X] Multiple algorithms for constructing the CEP graph
* [X] Generic dataset schema
* [X] Generic input/output interface (With support for File-based input/output)
* [ ] Negation operator support
* [ ] Kleene closure operator support
* [ ] "Partial sequence" support
* [ ] A variety of selection and consumption policies
* [ ] Performance optimizations based on the 'lazy evaluation' principle
* [ ] Adaptive complex event processing
* [ ] Multi-pattern support
* [ ] Parallel execution support

# How to Use
* The "main" class of this library is the CEP class (CEP.py).
* Users are expected to create a CEP object and then invoke it on an event stream to obtain the pattern matches.
* The CEP object is initialized with a list of patterns to be detected and a set of configurable parameters. As of this writing, the only such parameter is the algorithm for constructing the evaluation tree.
* To create an event stream, you can manually create an empty stream and add events to it, and you can also provide a csv file to the fileInput function.
* To handle the CEP output, you can manually read the events from the CEP object or from the matches container, or use the fileOutput function to print the matches into a file.
* To create a pattern, the following components must be specified:
    * The pattern structure - e.g., SEQ(A, B, C) or AND(X, Y).
    * The formula that must be satisfied by the atomic items in the pattern structure.
    * The time window within which the atomic items in the pattern structure should appear in the stream.
* Allows to receive data asynchronously.
* Allows to receive data asynchronously.
* To use the asynchronous run, you need to insert the parameter is_async=True to the CEP.run method
```
cep.run(stream, is_async=True, "output_file.txt", streaming.__time_limit)
```

# Examples
Defining a pattern:
```
# This pattern is looking for a short ascend in the Google peak prices.
# PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b, GoogleStockPriceUpdate c)
# WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
# WITHIN 3 minutes
googleAscendPattern = Pattern(
        SeqOperator([QItem("GOOG", "a"), QItem("GOOG", "b"), QItem("GOOG", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )


# This pattern is looking for low prices of Amazon and Google at the same minute.
# PATTERN AND(AmazonStockPriceUpdate a, GoogleStockPriceUpdate g)
# WHERE a.PeakPrice <= 73 AND g.PeakPrice <= 525
# WITHIN 1 minute
googleAmazonLowPattern = Pattern(
    AndOperator([QItem("AMZN", "a"), QItem("GOOG", "g")]),
    AndFormula(
        SmallerThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(73)),
        SmallerThanEqFormula(IdentifierTerm("g", lambda x: x["Peak Price"]), AtomicTerm(525))
    ),
    timedelta(minutes=1)
)
```

Creating a CEP object for monitoring the patterns from the example above:
```
cep = CEP([googleAscendPattern, googleAmazonLowPattern], 
          EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE, None)
```

Defining a new file-based event stream formatted according to Metastock 7 format:
```
events = file_input("test/EventFiles/NASDAQ_SHORT.txt", MetastockDataFormatter())
```

Applying an existing CEP object on an event stream and storing the resulting pattern matches to a file:
```
cep.run(events) # potentially blocking call
matches = cep.get_pattern_match_stream()
file_output(matches, 'output.txt')
```

#Twitter Stream
###<ins>Authentication</ins>
In order to receive and use a stream from twitter, credentials are needed. Insert your credentials in TwitterCredentials.py
###<ins>Creating a twitter stream</ins>
To create a twitter stream, a creation of TweetsStreamSessionInput class is needed. After creating the class above, use the get_stream_queue method while supplying a list of words as parameters that will determine the income tweets through the stream.
###<ins>Tweet formation in CEP</ins>
The formation of a tweet is defined in Tweets.py (see documentation). The tweet keys are described there based on the overview of a tweet in https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
###<ins>Using the timeout feature</ins>
In order to use the timeout feature, insert a timeout parameter when creating a TweetsStreamSessionInput.
For example: TweetsStreamSessionInput(time_out=10) if you want to stop receiving data from the stream after 10 seconds and stop the CEP run
# Examples
Creating a TweetsStreamSessionInput object:
time_limit is time (in seconds) you want the streaming will run (optional).
```
streaming = TweetsStreamSessionInput(time_limit=x)
```
Get the stream queue (Stream object) of the tweets. 
In this example we search tweets that include the word "corona":
```
stream_queue = streaming.get_stream_queue(['corona'])
```
After you created a queue, you can run the CEP. 
Provide a path to the file you want the results will print to, and the time_limit above (optional):
```
cep = CEP([pattern],
          EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE, None)
cep.run(stream_queue, is_async=True, file_path="output.txt", time_limit=x)
```