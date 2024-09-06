'''
Example to demonstrate usage the AWS Kinesis Video Streams (KVS) Audio Consumer Library for Python.
 '''
 
__version__ = "0.0.2"
__status__ = "Development"
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>, Nicolas Neudeck <https://www.linkedin.com/in/nicolasneudeck/>"

from io import BytesIO
import os
import sys
import time
import boto3
import logging
from pydub import AudioSegment
from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import KvsConsumerLibrary
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragementProcessor

# Config the logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

# Update the desired region and KVS stream name.
REGION='[YOUR_REGION]'  # Region where the KVS stream is located
KVS_STREAM_NAME = '[YOUR_STREAM_NAME]'   # Stream must be in specified region
FRAGMENT_NUMBER = '[YOUR_FRAGMENT_NUMBER]'     # Fragment number to start reading from


class KvsPythonConsumerExample:
    '''
    Example class to demonstrate usage the AWS Kinesis Video Streams KVS) Consumer Library for Python.
    '''

    def __init__(self):
        '''
        Initialize the KVS clients as needed. The KVS Comsumer Library intentionally does not abstract 
        the KVS clients or the various media API calls. These have individual authentication configuration and 
        a variety of other user defined settings so we keep them here in the users application logic for configurability.

        The KvsConsumerLibrary sits above these and parses responses from GetMedia and GetMediaForFragmentList 
        into MKV fragments and provides convenience functions to further process, save and extract individual frames.  
        '''

        # Create shared instance of KvsFragementProcessor
        self.kvs_fragment_processor = KvsFragementProcessor()

        # Variable to maintaun state of last good fragememt mostly for error and exception handling.
        self.last_good_fragment_tags = None

        # Init the KVS Service Client and get the accounts KVS service endpoint
        log.info('Initializing Amazon Kinesis Video client....')
        # Attach session specific configuration (such as the authentication pattern)
        self.session = boto3.Session(region_name=REGION)
        self.kvs_client = self.session.client("kinesisvideo")

        # Initialize the audio track bytearrays
        self.audio_to_customer = bytearray()
        self.audio_from_customer = bytearray()

        self.current_audio_length = 0.0

        self.stream_stopped = False

    ####################################################
    # Main process loop
    def service_loop(self):
        
        ####################################################
        # Start an instance of the KvsConsumerLibrary reading in a Kinesis Video Stream

        # Get the KVS Endpoint for the GetMedia Call for this stream
        log.info(
            "Getting KVS GetMedia Endpoint for stream: {} ........".format(
                KVS_STREAM_NAME
            )
        )
        get_media_endpoint = self._get_data_endpoint(
            KVS_STREAM_NAME, "GET_MEDIA"
        )
        
        # Get the KVS Media client for the GetMedia API call
        log.info(f'Initializing KVS Media client for stream: {KVS_STREAM_NAME}........') 
        kvs_media_client = self.session.client('kinesis-video-media', endpoint_url=get_media_endpoint)

        # Make a KVS GetMedia API call with the desired KVS stream and StartSelector type and time bounding.
        log.info(f'Requesting KVS GetMedia Response for stream: {KVS_STREAM_NAME}........')


        # get_media_response = kvs_media_client.get_media(
        #     StreamName=KVS_STREAM_NAME,
        #     StartSelector={
        #         'StartSelectorType': 'NOW'
        #     }
        # )

        get_media_response = kvs_media_client.get_media(
            StreamName=KVS_STREAM_NAME,
            StartSelector={
                "StartSelectorType": "FRAGMENT_NUMBER",
                "AfterFragmentNumber": FRAGMENT_NUMBER,
            },
        )

        # Initialize an instance of the KvsConsumerLibrary, provide the GetMedia response and the required call-backs
        log.info(f'Starting KvsConsumerLibrary for stream: {KVS_STREAM_NAME}........') 
        my_stream01_consumer = KvsConsumerLibrary(KVS_STREAM_NAME, 
                                              get_media_response, 
                                              self.on_fragment_arrived, 
                                              self.on_stream_read_complete, 
                                              self.on_stream_read_exception
                                            )

        # Start the instance of KvsConsumerLibrary, any matching fragments will begin arriving in the on_fragment_arrived callback
        my_stream01_consumer.start()

        # Can create another instance of KvsConsumerLibrary on a different media stream or continue on to other application logic. 

        # Here can hold the process up by waiting for the KvsConsumerLibrary thread to finish (may never finish for live streaming fragments)
        #my_stream01_consumer.join()

        # Or 
    
        # Run a loop with the applications main functionality that holds the process open.
        # Can also use to monitor the completion of the KvsConsumerLibrary instance and trigger a required action on completion.
        while not self.stream_stopped:

            #Add Main process / application logic here while KvsConsumerLibrary instance runs as a thread
            log.info("Nothn to see, just doin main application stuff in a loop here!")
            time.sleep(2)
            
            # Call below to exit the streaming get_media() thread gracefully before reaching end of stream. 
            #my_stream01_consumer.stop_thread()

        my_stream01_consumer.join()
        print('KvsConsumerLibrary Thread has exited - Main Application Loop will now exit.')


    ####################################################
    # KVS Consumer Library call-backs

    def on_fragment_arrived(self, stream_name, fragment_bytes, fragment_dom, fragment_receive_duration):
        '''
        This is the callback for the KvsConsumerLibrary to send MKV fragments as they are received from a stream being processed.
        The KvsConsumerLibrary returns the received fragment as raw bytes and a DOM like structure containing the fragments meta data.

        With these parameters you can do a variety of post-processing including saving the fragment as a standalone MKV file
        to local disk, request individual frames as a numpy.ndarray for data science applications or as JPEG/PNG files to save to disk 
        or pass to computer vison solutions. Finally, you can also use the Fragment DOM to access Meta-Data such as the MKV tags as well
        as track ID and codec information. 

        In the below example we provide a demonstration of all of these described functions.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **fragment_bytes**: bytearray
                A ByteArray with raw bytes from exactly one fragment. Can be save or processed to access individual frames

            **fragment_dom**: mkv_fragment_doc: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                A DOM like structure of the parsed fragment providing searchable list of EBML elements and MetaData in the Fragment

            **fragment_receive_duration**: float
                The time in seconds that the fragment took for the streaming data to be received and processed. 
        
        '''
        
        try:
            # Log the arrival of a fragment. 
            # use stream_name to identify fragments where multiple instances of the KvsConsumerLibrary are running on different streams.
            log.info(f'\n\n##########################\nFragment Received on Stream: {stream_name}\n##########################')
            
            # Print the fragment receive and processing duration as measured by the KvsConsumerLibrary
            log.info('')
            log.info(f'####### Fragment Receive and Processing Duration: {fragment_receive_duration} Secs')

            # Get the fragment tags and save in local parameter.
            self.last_good_fragment_tags = self.kvs_fragment_processor.get_fragment_tags(fragment_dom)

            # Get the fragment number and save in local parameter.
            stream_fragment_number = self.last_good_fragment_tags[
                "AWS_KINESISVIDEO_CONTINUATION_TOKEN"
            ]

            ##### Log Time Deltas:  local time Vs fragment SERVER and PRODUCER Timestamp:
            time_now = time.time()
            kvs_ms_behind_live = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_MILLIS_BEHIND_NOW'])
            producer_timestamp = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_PRODUCER_TIMESTAMP'])
            server_timestamp = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_SERVER_TIMESTAMP'])
            
            log.info('')
            log.info('####### Timestamps and Delta: ')
            log.info(f'KVS Reported Time Behind Live {kvs_ms_behind_live} mS')
            log.info(f'Local Time Diff to Fragment Producer Timestamp: {round(((time_now - producer_timestamp)*1000), 3)} mS')
            log.info(f'Local Time Diff to Fragment Server Timestamp: {round(((time_now - server_timestamp)*1000), 3)} mS')

            ###########################################
            # 1) Extract and print the MKV Tags in the fragment
            ###########################################
            # Get the fragment MKV Tags (Meta-Data). KVS allows these to be set per fragment to convey some information 
            # about the attached frames such as location or Computer Vision labels. Here we just log them!
            log.info('')
            log.info('####### Fragment MKV Tags:')
            for key, value in self.last_good_fragment_tags.items():
                log.info(f'{key} : {value}')

            ###########################################
            # 2) Pretty Print the entire fragment DOM structure
            # ###########################################
            # Get and log the the pretty print string for entire fragment DOM structure from EBMLite parsing.
            log.info('')
            log.info('####### Pretty Print Fragment DOM: #######')
            pretty_frag_dom = self.kvs_fragment_processor.get_fragement_dom_pretty_string(fragment_dom)
            log.info(pretty_frag_dom)

            
            ###########################################
            # 3) add audio to customer and agent
            ###########################################
            simple_block_elements = self.kvs_fragment_processor.get_simple_block_offset(
                fragment_dom
            )
            audio_from_customer_track, audio_to_customer_track = (
                self.kvs_fragment_processor.get_audio_tracks(fragment_dom)
            )
            log.debug(f"audio_from_customer_track: {audio_from_customer_track}")
            log.debug(f"audio_to_customer_track: {audio_to_customer_track}")
            for offset, size in simple_block_elements:
                track_number, data_payload = (
                    self.kvs_fragment_processor.extract_simpleblock_data(
                        fragment_bytes[offset : (offset + size)]
                    )
                )
                if track_number == audio_from_customer_track:
                    self.audio_from_customer.extend(data_payload)
                elif track_number == audio_to_customer_track:
                    self.audio_to_customer.extend(data_payload)
                length = float(size) / 2.0 / 8000.0
                self.current_audio_length += length
                log.debug(f"Audio Length: {self.current_audio_length}")

        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')
    
    def on_stream_read_complete(self, stream_name):
        '''
        This callback is triggered by the KvsConsumerLibrary when a stream has no more fragments available.
        This represents a graceful exit of the KvsConsumerLibrary thread.

        A stream will reach the end of the available fragments if the StreamSelector applied some 
        time or fragment bounding on the media request or if requesting a live steam and the producer 
        stopped sending more fragments. 

        Here you can choose to either restart reading the stream at a new time or just clean up any
        resources that were expecting to process any further fragments. 
        
        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.
        '''

        # Do something here to tell the application that reading from the stream ended gracefully.
        print(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')
        self.save_audio_files(self.audio_to_customer, self.audio_from_customer)
        self.stream_stopped = True

    def save_audio_files(self, audio_to_customer, audio_from_customer):
        min_length = min(len(audio_to_customer), len(audio_from_customer))
        channels = 1
        sample_width = 2
        sample_rate = 8000
        audio_to_customer_audio_stream = BytesIO(bytes(audio_to_customer[:min_length]))
        audio_to_customer_audio_segment = AudioSegment.from_file(
            audio_to_customer_audio_stream,
            format="raw",
            codec="pcm_s16le",
            frame_rate=sample_rate,
            channels=channels,
            sample_width=sample_width,
        )

        audio_from_customer_audio_stream = BytesIO(
            bytes(audio_from_customer[:min_length])
        )
        audio_from_customer_audio_segment = AudioSegment.from_file(
            audio_from_customer_audio_stream,
            format="raw",
            codec="pcm_s16le",
            frame_rate=sample_rate,
            channels=channels,
            sample_width=sample_width,
        )

        # if the audio is not the same length, we need to add silence to the shorter one
        max_length = max(
            len(audio_to_customer_audio_segment), len(audio_from_customer_audio_segment)
        )
        audio_to_customer_audio_segment = (
            audio_to_customer_audio_segment
            + AudioSegment.silent(
                duration=max_length - len(audio_to_customer_audio_segment),
                frame_rate=sample_rate,
            )
        )
        audio_from_customer_audio_segment = (
            audio_from_customer_audio_segment
            + AudioSegment.silent(
                duration=max_length - len(audio_from_customer_audio_segment),
                frame_rate=sample_rate,
            )
        )


        # MONO (agent and customer audio combined)
        # combined_audio1_file_name = "combined_mono_audio.wav"
        # combined_audio1 = audio_to_customer_audio_segment.overlay(
        #     audio_from_customer_audio_segment
        # )
        # combined_audio1.export(combined_audio1_file_name, format='wav')

        # STEREO (agent on left channel, customer on right channel - good for transcription)
        combined_audio_file_name = "combined_stereo_audio.wav"
        try:
            combined_audio = AudioSegment.from_mono_audiosegments(
                audio_from_customer_audio_segment, audio_to_customer_audio_segment
            )
            combined_audio.export(combined_audio_file_name, format='wav')
        except Exception as e:
            log.error(f"Error combining audio: {e}")
        

    def on_stream_read_exception(self, stream_name, error):
        '''
        This callback is triggered by an exception in the KvsConsumerLibrary reading a stream. 
        
        For example, to process use the last good fragment number from self.last_good_fragment_tags to
        restart the stream from that point in time with the example stream selector provided below. 
        
        Alternatively, just handle the failed stream as per your application logic requirements.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **error**: err / exception
                The Exception obje tvthat was thrown to trigger this callback.

        '''

        # Can choose to restart the KvsConsumerLibrary thread at the last received fragment with below example StartSelector
        #StartSelector={
        #    'StartSelectorType': 'FRAGMENT_NUMBER',
        #    'AfterFragmentNumber': self.last_good_fragment_tags['AWS_KINESISVIDEO_CONTINUATION_TOKEN'],
        #}

        # Here we just log the error 
        print(f'####### ERROR: Exception on read stream: {stream_name}\n####### Fragment Tags:\n{self.last_good_fragment_tags}\nError Message:{error}')

    ####################################################
    # KVS Helpers
    def _get_data_endpoint(self, stream_name, api_name):
        '''
        Convenience method to get the KVS client endpoint for specific API calls. 
        '''
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

if __name__ == "__main__":
    '''
    Main method for example KvsConsumerLibrary
    '''
    
    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()

