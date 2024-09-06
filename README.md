# Amazon KVS Audio Consumer Library For Python

## Introduction

The Amazon KVS Audio Consumer Library for Python reads in streaming bytes from Amazon 
Kinesis Video Streams (KVS) made available via a KVS GetMedia or GetMediaForFragmentList API call response. 
The library parses these raw bytes into individual MKV fragments and forwards to call-backs in the user’s application.

Fragments are returned as raw bytes and a searchable DOM like structure by parsing with EMBLite by MideTechnology.

In addition, the KvsFragementProcessor class provides the following functions for post-processing of parsed MKV fragments:
1) get_fragment_tags(): Extract MKV tags from the fragment.
2) save_fragment_as_local_mkv(): Saves the fragment as stand-alone MKV file on local disk.
3) get_audio_tracks(): Get the track names for audio from different sources.
4) extract_simpleblock_data(): get the elements in the MKV datastructure that contain the raw audio data
5) parse_vint(): Parse an EBML variable size integer (vint) used in Matroska.


## Getting started

A complete example of how to consume the Amazon Kinesis Video Stream Consumer Library for Python is provided in the 
[kvs_consumer_library_example](kvs_consumer_library_example.py) module.

### To deploy this example:
1. Make sure you have an stream in KVS. A stream consist of a stream name, a Fragment number from where to start the stream and the region in which this stream is located.

2. Clone and CD into this repository
```
git clone https://github.com/heushreck/amazon-kvs-audio-consumer-library-for-python.git
cd amazon-kvs-audio-consumer-library-for-python
```

1. Active your Virtual Environment and install Python Dependencies:
```
python -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

1. Open the cloned repository with your favourite IDE 

2. In kvs_consumer_library_example.py, update the KVS stream parameters:  
    a. REGION  
    b. KVS_STREAM01_NAME
    c. FRAGMENT_NUMBER 

3. Run the example code:
```
python3 kvs_consumer_library_example.py
```

This assumes default client authentication and so your host machine must have a valid AWS credentials file or receive temporary credentials by other means. 
User IAM or temporary credentials must have AmazonKinesisVideoStreamsReadOnlyAccess or some per stream specific equivalent of these permissions. 

Assuming authenticating is successful then the consumer library will be reading in the nominated KVS stream and returning parsed MKV fragments to the on_fragment_arrived() callback where a series of post-processing of the fragment is completed.

Check the on_fragment_arrived function and see the post processing features.
Check out the save_audio_files function which is called once the stream ends to see how the audio file is generated and saved to disk.

## Summary Workflow

1) Define a on_fragment_arrived() and on_read_stream_complete() and on_stream_read_exception() call-backs in user application logic.
2) Initialize the KVS Media and / or Archive Media clients,
3) Make a call to KVS Media GetMedia and / or KVS Archive Media GetMediaForFragmentList for the given stream,
4) Initialize and run this KVS Consumer library thread providing the response from the GetMedia
or GetMediaForFragmentList call,
5) Fragments will then be parsed and delivered to the call-backs for processing as per the example code provided.

## Timing and Async Considerations

To keep the examples and base solution as simple as possible, the fragment processing library provided is threaded to 
run outside of the main process but it returns received fragments to the main application process call-backs and isn't
using any asynchronous programming techniques. Therefore, any processing time taken in the on_fragment_arrived() callback
will be blocking for the KVS consumer library fragment processing function. If the processing takes longer (or close to) than the 
fragment duration then the stream processing will slip behind the live edge of the media and introduce additional latency.  

If performing long or external blocking processes in the on_fragment_arrived() callback, it is the responsibility of the 
developer to thread or develop async solutions to prevent extended blocking of the consumer library fragment processing. 

## Credits:

[Dean Colcott](https://www.linkedin.com/in/deancolcott/) from AWS who provided the sample repository processing video data instead of audio data.

[EMBLite by MideTechnology](https://github.com/MideTechnology/ebmlite) is an external EBML parser used to decode the MKV fragents in this library.
For convenance, a slightly modified version of EMBLite is shipped with the KvsConsumerLibrary but adding credit where its due.  
EMBLite MIT License: https://github.com/MideTechnology/ebmlite/blob/development/LICENSE  



