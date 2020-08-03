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

## Negation Operator 

The following is the example of a pattern containing a negation operator:

```
pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), NegationOperator(QItem("AMZN", "b")), QItem("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )


# Advanced configuration settings
## Consumption policies and selection strategies

OpenCEP supports a variety of consumption policies provided using the ConsumptionPolicy parameter in the pattern definition.

The following pattern definition limiting all primitive events to only appear in a single full match.
```
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    TrueFormula(),
    timedelta(minutes=5),
    ConsumptionPolicy(primary_selection_strategy = SelectionStrategies.MATCH_SINGLE)
)
```
This selection strategy further limits the pattern detection process, only allowing to match produce a single intermediate partial match containing an event. 
```
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    TrueFormula(),
    timedelta(minutes=5),
    ConsumptionPolicy(primary_selection_strategy = SelectionStrategies.MATCH_NEXT)
)
```
It is also possible to enforce either MATCH_NEXT or MATCH_SINGLE on a subset of event types. 
```
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    TrueFormula(),
    timedelta(minutes=5),
    ConsumptionPolicy(single=["AMZN", "AVID"], 
                        secondary_selection_strategy = SelectionStrategies.MATCH_NEXT)
)
```
This consumption policy specifies a list of events that must be contiguous in the input stream, i.e., 
no other unrelated event is allowed to appear in between.
```
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    TrueFormula(),
    timedelta(minutes=5),
    ConsumptionPolicy(contiguous=["a", "b", "c"])
)
```
The following example instructs the framework to prohibit creation of new partial matches
from the point a new "b" event is accepted and until it is either matched or expired.

```
# Enforce mechanism from the first event in the sequence
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    AndFormula(
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])), 
        GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
    timedelta(minutes=5),
    ConsumptionPolicy(freeze="b")
)
```

## Optimizing evaluation performance by specifying custom TreeStorageParameters
```
storage_params = TreeStorageParameters(sort_storage=True,
  attributes_priorities={"a": 122, "b": 200, "c": 104, "m": 139})

""" 
sort_storage: configures the storage to sort the PMs (sort_storage == TRUE).
attributes_priorities: in case of a AND between two formulas we sort according to the attributes which are specified with the higher priority - wise to specify priorities according to the events frequencies and the expected percentage of the attributes which will NOT fullfil the condition for example if a is frequent and rarely his condition is met it is wise to specify a with high priority. notice that the priority is per attribute.
clean_expired_every: a number that determines the frequency of cleaning up old partial matches from storage units.
sort_by_condition_on_attributes: in case the pattern is a sequence(SeqOperator) but sorting by the given condition on certain attributes is more efficient then use (sort_by_condition_on_attributes=True), in case sorting keys aren't extractable from the condition then sorting will be by timestamps.
"""


eval_mechanism_params=TreeBasedEvaluationMechanismParameters(storage_params=storage_params)

#can be any tree based evaluation parameters, just make sure to provide storage_params


cep = CEP(pattern, EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
        eval_mechanism_params)
```


# Twitter API support
### Authentication
To receive a Twitter stream via Twitter API, provide your credentials in plugin/twitter/TwitterCredentials.py
### Creating a twitter stream
To create a twitter stream, a creation of TweetsStreamSessionInput class is needed. After creating the class above, use the get_stream_queue method while supplying a list of words as parameters that will determine the income tweets through the stream.
### Tweet formation in CEP
The formation of a tweet is defined in Tweets.py (see documentation). The tweet keys are described there based on the overview of a tweet in https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
### Using the timeout feature
In order to use the timeout feature, insert a timeout parameter when creating a TweetsStreamSessionInput.
For example: TweetsStreamSessionInput(time_out=10) if you want to stop receiving data from the stream after 10 seconds and stop the CEP run
### Examples
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
