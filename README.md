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

### Three different, independent mechanisms for providing "less-than-all-possible-results" semantics ###

First mechanism: limiting a primitive event to only appear in a single full match
```
pattern = Pattern(
    StrictSeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    TrueFormula(),
    timedelta(minutes=5),
    ConsumptionPolicies(single = {"AAPL", "AMZN"})# Prevent events  of type "AAPL" or "AMZN" from appearing twice
)
```

Second mechanism: Defining strict patterns:
```
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    TrueFormula(),
    timedelta(minutes=5),
    ConsumptionPolicies(strict = {"a", "b", "c"})
)
```

Alternatively, each pair of successive event types can be manually defined to skip any number of events in between:
```
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    AndFormula(
        EqFormula(
            IdentifierTerm("a", lambda x: x["Index"]), 
            MinusTerm(IdentifierTerm("b", lambda x: x["Index"]), AtomicTerm(1))
        ),
        EqFormula(
            IdentifierTerm("b", lambda x: x["Index"]), 
            MinusTerm(IdentifierTerm("c", lambda x: x["Index"]), AtomicTerm(1))
        )
    ),
    timedelta(minutes=5)
)
```

Third mechanism: Prohibiting any pattern matching while a partial match is active,
while a user is able to define a specific point in a sequence from which this behavior must be enforced:

```
# Enforce mechanism from the first event in the sequence
pattern = Pattern(
    SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]), 
    AndFormula(
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])), 
        GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
    timedelta(minutes=5),
    ConsumptionPolicies(skip = "b")
)
```
